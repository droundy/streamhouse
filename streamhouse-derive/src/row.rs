use proc_macro2::{Ident, TokenStream};
use quote::{quote, ToTokens};
use syn::{Data, DataEnum, DataStruct, Expr, ExprLit, Fields, Lit, Type};

pub(crate) enum RowStruct {
    Named {
        name: Ident,
        field_names: Vec<Ident>,
        field_types: Vec<Type>,
    },
    Unnamed {
        name: Ident,
        field_type: Type,
    },
    Enum {
        name: Ident,
        variants: Vec<Ident>,
        values: Vec<i8>,
    },
}

impl RowStruct {
    pub fn parse(name: &Ident, data: &Data) -> Self {
        let name = name.clone();

        match data {
            syn::Data::Struct(DataStruct {
                fields: Fields::Named(fields),
                ..
            }) => RowStruct::Named {
                name,
                field_names: fields.named.iter().flat_map(|f| f.ident.clone()).collect(),
                field_types: fields.named.iter().map(|f| f.ty.clone()).collect(),
            },
            syn::Data::Struct(DataStruct {
                fields: Fields::Unnamed(fields),
                ..
            }) => {
                if fields.unnamed.len() != 1 {
                    panic!("Row can only support a single unnamed field");
                }
                RowStruct::Unnamed {
                    name,
                    field_type: fields.unnamed.first().unwrap().ty.clone(),
                }
            }
            syn::Data::Enum(DataEnum { variants, .. }) => {
                let v = variants.iter().cloned().collect::<Vec<_>>();
                let variants = v.iter().map(|v| v.ident.clone()).collect::<Vec<_>>();
                let mut values = Vec::new();
                let mut last_value = 0;
                for variant in v {
                    if let Some((
                        _,
                        Expr::Lit(ExprLit {
                            lit: Lit::Int(discriminant),
                            ..
                        }),
                    )) = variant.discriminant
                    {
                        let disc = discriminant
                            .base10_parse::<i8>()
                            .expect("Invalid discriminant");
                        values.push(disc);
                        last_value = disc;
                    } else {
                        values.push(last_value + 1);
                        last_value += 1;
                    }
                }
                RowStruct::Enum {
                    name,
                    variants,
                    values,
                }
            }
            _ => panic!("Row cannot support unit structs"),
        }
    }
}

impl ToTokens for RowStruct {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            RowStruct::Named {
                name,
                field_names,
                field_types,
            } => {
                let field_name_strs = field_names
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();

                tokens.extend(
                    [quote! {
                        impl ::streamhouse::Row for #name {
                            fn columns(_parent: &'static str) -> Vec<::streamhouse::internal::Column> {
                                let mut out = Vec::new();
                                #(out.extend(<#field_types as ::streamhouse::Row>::columns(#field_name_strs));)*
                                out
                            }
                            fn read(buf: &mut ::streamhouse::internal::Bytes) -> Result<Self, ::streamhouse::Error> {
                                #(let #field_names = buf.read()?;)*
                                Ok(#name { #(#field_names),* })
                            }
                            fn write(&self, buf: &mut impl ::streamhouse::internal::WriteRowBinary) -> Result<(), ::streamhouse::Error> {
                                use ::streamhouse::Row;
                                #(self.#field_names.write(buf)?;)*
                                Ok(())
                            }
                        }
                    }]
                    .into_iter(),
                );
            }
            RowStruct::Unnamed { name, field_type } => {
                tokens.extend(
                    [quote! {
                        impl ::streamhouse::Row for #name {
                            fn columns(parent: &'static str) -> Vec<::streamhouse::internal::Column> {
                                <#field_type as ::streamhouse::Row>::columns(parent)
                            }
                            fn read(buf: &mut ::streamhouse::internal::Bytes) -> Result<Self, ::streamhouse::Error> {
                                Ok(#name ( buf.read()? ))
                            }
                            fn write(&self, buf: &mut impl ::streamhouse::internal::WriteRowBinary) -> Result<(), ::streamhouse::Error> {
                                ::streamhouse::Row::write(&self.0, buf)
                            }
                        }
                    }]
                    .into_iter(),
                );
            }
            RowStruct::Enum {
                name,
                variants,
                values,
            } => {
                let variants_str = variants
                    .iter()
                    .map(|i| i.to_string())
                    .zip(values)
                    .map(|(v, val)| format!("'{v}' = {val}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let clickhouse_type = format!("Enum8({variants_str})");
                tokens.extend(
                    [quote! {
                        impl ::streamhouse::Row for #name {
                            fn columns(parent: &'static str) -> Vec<::streamhouse::internal::Column> {
                                vec![::streamhouse::internal::Column::new(parent, #clickhouse_type)]
                            }
                            fn read(buf: &mut ::streamhouse::internal::Bytes) -> Result<Self, ::streamhouse::Error> {
                                let x: i8 = buf.read()?;
                                match x {
                                    #(
                                        #values => Ok(Self::#variants),
                                    )*
                                    _ => Err(::streamhouse::Error::InvalidTagEncoding(x))
                                }
                            }
                            fn write(&self, buf: &mut impl ::streamhouse::internal::WriteRowBinary) -> Result<(), ::streamhouse::Error> {
                                match self {
                                    #(
                                        Self::#variants => ::streamhouse::Row::write(&(#values as i8), buf),
                                    )*
                                }
                            }
                        }
                    }]
                    .into_iter(),
                );
            }
        }
    }
}
