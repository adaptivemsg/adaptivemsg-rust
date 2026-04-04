use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::Parser;
use syn::{parse_macro_input, Fields, ItemImpl, ItemStruct, LitStr};

fn compile_error<T: quote::ToTokens>(tokens: T, message: &str) -> TokenStream {
    syn::Error::new_spanned(tokens, message)
        .to_compile_error()
        .into()
}

#[proc_macro_attribute]
pub fn message_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let Some((_, trait_path, _)) = input.trait_.as_ref() else {
        return compile_error(&input.self_ty, "message_handler must be used on an impl of MessageHandler");
    };
    let is_message_handler = trait_path
        .segments
        .last()
        .map(|seg| seg.ident == "MessageHandler")
        .unwrap_or(false);
    if !is_message_handler {
        return compile_error(trait_path, "message_handler must be used on an impl of MessageHandler");
    }
    if !input.generics.params.is_empty() {
        return compile_error(&input.generics, "message_handler does not support generic impls");
    }
    let ty = *input.self_ty.clone();
    let expanded = quote! {
        #[::adaptivemsg::async_trait]
        #input
        ::adaptivemsg::submit_message_handler!(#ty);
        ::adaptivemsg::submit_message!(#ty);
    };
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn message(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut ns: Option<LitStr> = None;
    let mut base_name: Option<LitStr> = None;
    let mut register: bool = false;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("ns") {
            let lit: LitStr = meta.value()?.parse()?;
            ns = Some(lit);
            return Ok(());
        }
        if meta.path.is_ident("name") {
            let lit: LitStr = meta.value()?.parse()?;
            base_name = Some(lit);
            return Ok(());
        }
        if meta.path.is_ident("register") {
            register = true;
            return Ok(());
        }
        Err(meta.error("unsupported message attribute; use ns=\"...\", name=\"...\", or register"))
    });
    if let Err(err) = parser.parse(attr.into()) {
        return err.to_compile_error().into();
    }

    let input = parse_macro_input!(item as ItemStruct);
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return compile_error(&input.generics, "message does not support generic structs");
    }
    let fields = match &input.fields {
        Fields::Named(fields) => fields,
        _ => {
            return compile_error(
                &input.ident,
                "message only supports structs with named fields",
            )
        }
    };
    let field_count = fields.named.len();
    let encode_fields = fields.named.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        quote! {
            items.push(::adaptivemsg::__private::rmpv::ext::to_value(&self.#ident)?);
        }
    });
    let decode_fields = fields.named.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;
        quote! {
            let #ident: #ty = ::adaptivemsg::__private::rmpv::ext::from_value(iter.next().unwrap())?;
        }
    });
    let init_fields = fields.named.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        quote! { #ident }
    });
    let ns_lit = ns.unwrap_or_else(|| LitStr::new("am", Span::call_site()));
    let base_expr = if let Some(base_name) = base_name {
        quote! { #base_name.to_string() }
    } else {
        quote! {{
            let module_leaf = ::core::module_path!()
                .rsplit("::")
                .next()
                .unwrap_or("unknown");
            format!("{}.{}", module_leaf, stringify!(#name))
        }}
    };
    let register_submit = if register {
        quote! { ::adaptivemsg::submit_message!(#name); }
    } else {
        quote! {}
    };
    let expanded = quote! {
        #[derive(::serde::Serialize, ::serde::Deserialize)]
        #input
        impl ::adaptivemsg::Message for #name {
            fn wire_name(&self) -> &'static str {
                Self::wire_name_static()
            }

            fn wire_name_static() -> &'static str {
                static WIRE_NAME: ::std::sync::OnceLock<String> = ::std::sync::OnceLock::new();
                WIRE_NAME.get_or_init(|| {
                    let ns = #ns_lit;
                    let base = #base_expr;
                    format!("{ns}.{base}")
                }).as_str()
            }

            fn encode_map(&self) -> ::std::result::Result<Vec<u8>, ::adaptivemsg::Error> {
                #[derive(::serde::Serialize)]
                struct Envelope<'a, T: ::serde::Serialize> {
                    r#type: &'a str,
                    data: &'a T,
                }
                let env = Envelope {
                    r#type: Self::wire_name_static(),
                    data: self,
                };
                ::adaptivemsg::__private::rmp_serde::to_vec_named(&env).map_err(::adaptivemsg::Error::from)
            }

            fn encode_compact(&self) -> ::std::result::Result<Vec<u8>, ::adaptivemsg::Error> {
                let mut items = Vec::with_capacity(1 + #field_count);
                items.push(::adaptivemsg::__private::rmpv::Value::String(::adaptivemsg::__private::rmpv::Utf8String::from(Self::wire_name_static())));
                #(#encode_fields)*
                let value = ::adaptivemsg::__private::rmpv::Value::Array(items);
                let mut buf = Vec::new();
                ::adaptivemsg::__private::rmpv::encode::write_value(&mut buf, &value)?;
                Ok(buf)
            }

            fn encode_postcard(&self) -> ::std::result::Result<Vec<u8>, ::adaptivemsg::Error> {
                ::adaptivemsg::__private::postcard::to_stdvec(self).map_err(::adaptivemsg::Error::from)
            }

            fn as_any(&self) -> &dyn ::core::any::Any {
                self
            }
        }

        impl ::adaptivemsg::__private::MessageDecode for #name {
            fn decode_map(value: ::adaptivemsg::__private::rmpv::Value) -> ::std::result::Result<Self, ::adaptivemsg::Error> {
                ::adaptivemsg::__private::rmpv::ext::from_value(value).map_err(::adaptivemsg::Error::from)
            }

            fn decode_compact(values: Vec<::adaptivemsg::__private::rmpv::Value>) -> ::std::result::Result<Self, ::adaptivemsg::Error> {
                if values.len() != #field_count {
                    return Err(::adaptivemsg::Error::CompactFieldCount {
                        expected: #field_count,
                        got: values.len(),
                    });
                }
                let mut iter = values.into_iter();
                #(#decode_fields)*
                Ok(Self { #(#init_fields),* })
            }

            fn decode_postcard(payload: &[u8]) -> ::std::result::Result<Self, ::adaptivemsg::Error> {
                ::adaptivemsg::__private::postcard::from_bytes(payload).map_err(::adaptivemsg::Error::from)
            }
        }
        #register_submit
    };
    TokenStream::from(expanded)
}
