use anyhow::{bail, Context, Result};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use syn::{
    parse_file, Attribute, Expr, ExprLit, Fields, GenericArgument, Item, Lit, LitStr, Meta,
    PathArguments, Type,
};

#[derive(Default)]
struct MessageAttr {
    ns: Option<String>,
    name: Option<String>,
}

struct MessageDef {
    name: String,
    fields: Vec<FieldDef>,
    wire_name: Option<String>,
}

struct FieldDef {
    name: String,
    ty: String,
    tag: String,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("amgen-rs: {err}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let input = parse_args()?;
    let input_abs = fs::canonicalize(&input)
        .or_else(|_| env::current_dir().map(|dir| dir.join(&input)))
        .with_context(|| format!("resolve input {}", input.display()))?;
    let output = default_output_path(&input_abs);
    let content = fs::read_to_string(&input_abs)
        .with_context(|| format!("read input {}", input_abs.display()))?;
    let file = parse_file(&content).context("parse Rust file")?;
    let (module_name, items) = find_message_scope(&file, &input_abs, &output)?;
    let package_name = package_name_from_output(&output)?;
    let force_wire_name = package_name != module_name;
    let messages = collect_messages(&module_name, &items, force_wire_name)?;
    if messages.is_empty() {
        bail!("no #[am::message] structs found in module {module_name}");
    }
    let rendered = render_go(&package_name, &messages);
    write_output(&output, rendered)?;
    if let Some(go_mod) = default_go_mod_path(&input_abs) {
        let module = default_go_module(&go_mod)?;
        let rendered_mod = render_go_mod(&module);
        write_output(&go_mod, rendered_mod)?;
    }
    Ok(())
}

fn parse_args() -> Result<PathBuf> {
    let mut input: Option<PathBuf> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--in" => {
                let value = args.next().context("--in requires a path")?;
                input = Some(PathBuf::from(value));
            }
            "-h" | "--help" => {
                print_usage();
                process::exit(0);
            }
            _ => bail!("unknown argument {arg}"),
        }
    }
    let Some(input) = input else {
        bail!("--in is required")
    };
    Ok(input)
}

fn print_usage() {
    println!("Usage: amgen-rs --in <message.rs>");
}

fn find_message_scope(
    file: &syn::File,
    input: &Path,
    output: &Path,
) -> Result<(String, Vec<Item>)> {
    let mut modules: Vec<(String, Vec<Item>)> = Vec::new();
    for item in &file.items {
        if let Item::Mod(item_mod) = item {
            if let Some((_, items)) = &item_mod.content {
                modules.push((item_mod.ident.to_string(), items.clone()));
            }
        }
    }
    if modules.len() > 1 {
        bail!("expected exactly one inline module containing messages");
    }
    if let Some(module) = modules.pop() {
        return Ok(module);
    }
    if let Some(module_name) = module_name_from_lib(input)? {
        return Ok((module_name, file.items.clone()));
    }
    let module_name = package_name_from_output(output)?;
    Ok((module_name, file.items.clone()))
}

fn package_name_from_output(output: &Path) -> Result<String> {
    if let Some(parent) = output.parent() {
        if let Some(name) = parent.file_name().and_then(|name| name.to_str()) {
            if !name.is_empty() {
                return Ok(name.to_string());
            }
        }
    }
    if let Some(stem) = output.file_stem().and_then(|name| name.to_str()) {
        if !stem.is_empty() {
            return Ok(stem.to_string());
        }
    }
    bail!("cannot infer package name from output path")
}

fn module_name_from_lib(input: &Path) -> Result<Option<String>> {
    let input_abs = fs::canonicalize(input).unwrap_or_else(|_| input.to_path_buf());
    let input_dir = match input_abs.parent() {
        Some(dir) => dir,
        None => return Ok(None),
    };
    let lib_path = input_dir.join("lib.rs");
    if !lib_path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&lib_path)
        .with_context(|| format!("read lib {}", lib_path.display()))?;
    let file = parse_file(&content).context("parse lib.rs")?;
    let input_rel = input_abs.strip_prefix(input_dir).unwrap_or(&input_abs);
    for item in file.items {
        let Item::Mod(item_mod) = item else {
            continue;
        };
        if item_mod.content.is_some() {
            continue;
        }
        for attr in &item_mod.attrs {
            let Meta::NameValue(meta) = &attr.meta else {
                continue;
            };
            if !meta.path.is_ident("path") {
                continue;
            }
            let Expr::Lit(ExprLit {
                lit: Lit::Str(path_lit),
                ..
            }) = &meta.value
            else {
                continue;
            };
            let attr_path = PathBuf::from(path_lit.value());
            if attr_path == input_rel || attr_path == input_abs {
                return Ok(Some(item_mod.ident.to_string()));
            }
        }
    }
    Ok(None)
}

fn default_output_path(input: &Path) -> PathBuf {
    // Write the generated Go file alongside the input Rust file.
    input.with_extension("go")
}

fn default_go_mod_path(input: &Path) -> Option<PathBuf> {
    if let Some(root) = repo_root_for_input(input) {
        return Some(root.join("go.mod"));
    }
    input.parent().map(|dir| dir.join("go.mod"))
}

fn repo_root_for_input(input: &Path) -> Option<PathBuf> {
    let mut dir = if input.is_dir() {
        input.to_path_buf()
    } else {
        input.parent()?.to_path_buf()
    };
    loop {
        if dir.join("Cargo.toml").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

fn default_go_module(go_mod: &Path) -> Result<String> {
    // Respect existing go.mod module name.
    if let Some(module) = parse_existing_go_mod(go_mod) {
        return Ok(module);
    }
    // Try to derive from .git/config origin URL.
    if let Some(module) = module_from_git_config(go_mod) {
        return Ok(module);
    }
    // Fallback to directory name.
    if let Some(parent) = go_mod.parent() {
        if let Some(name) = parent.file_name().and_then(|name| name.to_str()) {
            if !name.is_empty() {
                return Ok(name.to_string());
            }
        }
    }
    if let Ok(dir) = env::current_dir() {
        if let Some(name) = dir.file_name().and_then(|name| name.to_str()) {
            if !name.is_empty() {
                return Ok(name.to_string());
            }
        }
    }
    bail!("cannot infer module name for go.mod")
}

/// Read an existing go.mod and return its module name.
fn parse_existing_go_mod(go_mod: &Path) -> Option<String> {
    let content = fs::read_to_string(go_mod).ok()?;
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(module) = trimmed.strip_prefix("module ") {
            let module = module.trim();
            if !module.is_empty() {
                return Some(module.to_string());
            }
        }
    }
    None
}

/// Parse `[remote "origin"]` url from `.git/config` and convert to a Go module path.
fn module_from_git_config(go_mod: &Path) -> Option<String> {
    let root = go_mod.parent()?;
    let config = fs::read_to_string(root.join(".git/config")).ok()?;
    let mut in_origin = false;
    for line in config.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_origin = trimmed == r#"[remote "origin"]"#;
            continue;
        }
        if in_origin {
            if let Some(url) = trimmed.strip_prefix("url = ") {
                let url = url.trim();
                let module = url
                    .strip_prefix("https://")
                    .or_else(|| url.strip_prefix("http://"))
                    .unwrap_or(url);
                let module = module.strip_suffix(".git").unwrap_or(module);
                if !module.is_empty() {
                    return Some(module.to_string());
                }
            }
        }
    }
    None
}

fn collect_messages(
    module_name: &str,
    items: &[Item],
    force_wire_name: bool,
) -> Result<Vec<MessageDef>> {
    let mut out = Vec::new();
    for item in items {
        let Item::Struct(item_struct) = item else {
            continue;
        };
        let Some(attr) = find_message_attr(&item_struct.attrs)? else {
            continue;
        };
        let attrs = parse_message_attr(attr)?;
        let wire_name = compute_wire_name(
            module_name,
            &item_struct.ident.to_string(),
            &attrs,
            force_wire_name,
        );
        let fields = parse_fields(&item_struct.fields)?;
        out.push(MessageDef {
            name: item_struct.ident.to_string(),
            fields,
            wire_name,
        });
    }
    Ok(out)
}

fn find_message_attr(attrs: &[Attribute]) -> Result<Option<&Attribute>> {
    let mut found = None;
    for attr in attrs {
        if is_am_message_attr(attr) {
            if found.is_some() {
                bail!("multiple #[am::message] attributes are not supported")
            }
            found = Some(attr);
        }
    }
    Ok(found)
}

fn is_am_message_attr(attr: &Attribute) -> bool {
    let segments = &attr.path().segments;
    segments.len() == 2 && segments[0].ident == "am" && segments[1].ident == "message"
}

fn parse_message_attr(attr: &Attribute) -> Result<MessageAttr> {
    let mut out = MessageAttr::default();
    match &attr.meta {
        Meta::Path(_) => return Ok(out),
        Meta::List(_) => {}
        Meta::NameValue(_) => bail!("unsupported am::message attribute form"),
    }
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("ns") {
            let lit: LitStr = meta.value()?.parse()?;
            out.ns = Some(lit.value());
            return Ok(());
        }
        if meta.path.is_ident("name") {
            let lit: LitStr = meta.value()?.parse()?;
            out.name = Some(lit.value());
            return Ok(());
        }
        if meta.path.is_ident("register") {
            return Ok(());
        }
        Err(meta
            .error("unsupported am::message attribute; use ns=\"...\", name=\"...\", or register"))
    })?;
    Ok(out)
}

fn compute_wire_name(
    module_name: &str,
    type_name: &str,
    attr: &MessageAttr,
    force: bool,
) -> Option<String> {
    let ns = attr.ns.as_deref().unwrap_or("am");
    let base = match &attr.name {
        Some(name) => name.clone(),
        None => format!("{}.{}", module_name, type_name),
    };
    let has_override = attr.name.is_some() || attr.ns.as_deref().is_some_and(|ns| ns != "am");
    if force || has_override {
        Some(format!("{}.{}", ns, base))
    } else {
        None
    }
}

fn parse_fields(fields: &Fields) -> Result<Vec<FieldDef>> {
    let Fields::Named(named) = fields else {
        bail!("message structs must use named fields")
    };
    let mut out = Vec::new();
    for field in &named.named {
        let ident = field.ident.as_ref().context("unnamed field")?;
        let rust_name = ident.to_string();
        let tag = serde_tag(&field.attrs)?.unwrap_or_else(|| rust_name.clone());
        let go_name = to_go_field_name(&rust_name)?;
        let go_type = go_type(&field.ty)?;
        out.push(FieldDef {
            name: go_name,
            ty: go_type,
            tag,
        });
    }
    Ok(out)
}

fn serde_tag(attrs: &[Attribute]) -> Result<Option<String>> {
    let mut rename: Option<String> = None;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if rename.is_some() {
                    return Err(meta.error("duplicate serde(rename)"));
                }
                let lit: LitStr = meta.value()?.parse()?;
                rename = Some(lit.value());
                return Ok(());
            }
            if meta.path.is_ident("rename_all") {
                return Err(meta.error("serde(rename_all) is not supported; use per-field rename"));
            }
            if meta.path.is_ident("skip")
                || meta.path.is_ident("skip_serializing")
                || meta.path.is_ident("skip_deserializing")
            {
                return Err(meta.error("serde(skip) is not supported for amgen-rs"));
            }
            Ok(())
        })?;
    }
    Ok(rename)
}

fn to_go_field_name(name: &str) -> Result<String> {
    let parts: Vec<&str> = name.split('_').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        bail!("invalid field name {name}")
    }
    let mut out = String::new();
    for part in parts {
        let upper = part.to_ascii_uppercase();
        if is_initialism(&upper) {
            out.push_str(&upper);
            continue;
        }
        let mut chars = part.chars();
        let Some(first) = chars.next() else {
            continue;
        };
        out.push(first.to_ascii_uppercase());
        out.push_str(chars.as_str());
    }
    Ok(out)
}

fn is_initialism(value: &str) -> bool {
    matches!(
        value,
        "API"
            | "ASCII"
            | "CPU"
            | "CSS"
            | "DNS"
            | "EOF"
            | "GUID"
            | "HTML"
            | "HTTP"
            | "HTTPS"
            | "ID"
            | "IP"
            | "JSON"
            | "LHS"
            | "RHS"
            | "QPS"
            | "RAM"
            | "RPC"
            | "SLA"
            | "SMTP"
            | "SQL"
            | "SSH"
            | "TCP"
            | "TLS"
            | "TTL"
            | "UDP"
            | "UI"
            | "UID"
            | "UUID"
            | "URI"
            | "URL"
            | "UTF8"
            | "VM"
            | "XML"
            | "XMPP"
            | "XSRF"
            | "XSS"
    )
}

fn go_type(ty: &Type) -> Result<String> {
    match ty {
        Type::Path(path) => {
            if path.qself.is_some() {
                bail!("qualified types are not supported")
            }
            let Some(segment) = path.path.segments.last() else {
                bail!("empty type path")
            };
            let ident = segment.ident.to_string();
            match ident.as_str() {
                "String" => Ok("string".to_string()),
                "bool" => Ok("bool".to_string()),
                "i8" => Ok("int8".to_string()),
                "i16" => Ok("int16".to_string()),
                "i32" => Ok("int32".to_string()),
                "i64" => Ok("int64".to_string()),
                "u8" => Ok("uint8".to_string()),
                "u16" => Ok("uint16".to_string()),
                "u32" => Ok("uint32".to_string()),
                "u64" => Ok("uint64".to_string()),
                "f32" => Ok("float32".to_string()),
                "f64" => Ok("float64".to_string()),
                "isize" | "usize" | "i128" | "u128" => {
                    bail!("{ident} is not supported; use a fixed-width integer")
                }
                "Option" => {
                    let inner = single_generic_type(segment)?;
                    let inner_go = go_type(&inner)?;
                    Ok(format!("*{inner_go}"))
                }
                "Vec" => {
                    let inner = single_generic_type(segment)?;
                    let inner_go = go_type(&inner)?;
                    Ok(format!("[]{inner_go}"))
                }
                "BTreeMap" | "HashMap" => {
                    let (key, value) = two_generic_types(segment)?;
                    let key_go = go_type(&key)?;
                    let value_go = go_type(&value)?;
                    Ok(format!("map[{key_go}]{value_go}"))
                }
                _ => Ok(ident),
            }
        }
        Type::Array(array) => {
            let len = array_len(&array.len)?;
            let inner = go_type(&array.elem)?;
            Ok(format!("[{len}]{inner}"))
        }
        Type::Slice(slice) => {
            let inner = go_type(&slice.elem)?;
            Ok(format!("[]{inner}"))
        }
        Type::Paren(paren) => go_type(&paren.elem),
        Type::Group(group) => go_type(&group.elem),
        Type::Reference(_) => bail!("references are not supported; use owned types"),
        Type::Tuple(_) => bail!("tuple types are not supported"),
        Type::Ptr(_) => bail!("raw pointers are not supported"),
        Type::BareFn(_) => bail!("function types are not supported"),
        Type::Never(_) => bail!("never type is not supported"),
        Type::TraitObject(_) => bail!("trait objects are not supported"),
        Type::ImplTrait(_) => bail!("impl traits are not supported"),
        Type::Macro(_) => bail!("macro types are not supported"),
        Type::Infer(_) => bail!("inferred types are not supported"),
        _ => bail!("unsupported type"),
    }
}

fn single_generic_type(segment: &syn::PathSegment) -> Result<Type> {
    let args = match &segment.arguments {
        PathArguments::AngleBracketed(args) => &args.args,
        _ => bail!("generic type requires angle brackets"),
    };
    let mut types = args.iter().filter_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    });
    let first = types.next().context("missing generic type")?;
    if types.next().is_some() {
        bail!("expected a single generic type")
    }
    Ok(first)
}

fn two_generic_types(segment: &syn::PathSegment) -> Result<(Type, Type)> {
    let args = match &segment.arguments {
        PathArguments::AngleBracketed(args) => &args.args,
        _ => bail!("generic type requires angle brackets"),
    };
    let mut types = args.iter().filter_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    });
    let first = types.next().context("missing map key type")?;
    let second = types.next().context("missing map value type")?;
    if types.next().is_some() {
        bail!("expected exactly two generic types")
    }
    Ok((first, second))
}

fn array_len(expr: &Expr) -> Result<String> {
    let Expr::Lit(ExprLit {
        lit: Lit::Int(value),
        ..
    }) = expr
    else {
        bail!("array length must be an integer literal")
    };
    Ok(value.base10_digits().to_string())
}

fn render_go(package_name: &str, messages: &[MessageDef]) -> String {
    let mut out = String::new();
    push_line(&mut out, "// Code generated by amgen-rs; DO NOT EDIT.");
    push_line(&mut out, "");
    push_line(&mut out, &format!("package {package_name}"));
    push_line(&mut out, "");

    for (idx, msg) in messages.iter().enumerate() {
        if idx > 0 {
            push_line(&mut out, "");
        }
        push_line(&mut out, &format!("type {} struct {{", msg.name));
        for field in &msg.fields {
            let tag = field.tag.replace('"', "\\\"");
            push_line(
                &mut out,
                &format!("\t{} {} `am:\"{}\"`", field.name, field.ty, tag),
            );
        }
        push_line(&mut out, "}");
        if let Some(wire) = &msg.wire_name {
            push_line(&mut out, "");
            push_line(
                &mut out,
                &format!("func (*{}) WireName() string {{", msg.name),
            );
            push_line(&mut out, &format!("\treturn {}", format_go_string(wire)));
            push_line(&mut out, "}");
        }
    }
    push_line(&mut out, "");
    out
}

fn render_go_mod(module_name: &str) -> String {
    let mut out = String::new();
    push_line(&mut out, &format!("module {module_name}"));
    push_line(&mut out, "");
    push_line(&mut out, "go 1.22");
    out
}

fn push_line(out: &mut String, line: &str) {
    out.push_str(line);
    out.push('\n');
}

fn format_go_string(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn write_output(path: &Path, contents: String) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("write output {}", path.display()))?;
    Ok(())
}
