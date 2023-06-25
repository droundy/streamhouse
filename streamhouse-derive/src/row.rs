use proc_macro2::{Ident, TokenStream};
use quote::{quote, ToTokens};
use syn::{ Data, DataStruct, Fields, Type};

pub(crate) struct RowStruct {
    name: Ident,
    field_names: Vec<Ident>,
    field_types: Vec<Type>,
}

impl RowStruct {
    pub fn parse(name: &Ident, data: &Data) -> Self {
        let name = name.clone();

        let fields = match data {
            syn::Data::Struct(DataStruct {
                fields: Fields::Named(fields),
                ..
            }) => fields,
            _ => panic!("Row supports only named struct"),
        };

        RowStruct {
            name,
            field_names: fields.named.iter().flat_map(|f| f.ident.clone()).collect(),
            field_types: fields.named.iter().map(|f| f.ty.clone()).collect(),
        }
    }
}

impl ToTokens for RowStruct {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let name = &self.name;
        let field_names = &self.field_names;
        let field_name_strs = self.field_names.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let field_types = &self.field_types;

        tokens.extend(
            [quote! {
                impl ::streamhouse::Row for #name {
                    const TYPES: &'static [::streamhouse::ColumnType] = &[#(<#field_types as ::streamhouse::Column>::TYPE, )*];
                    const NAMES: &'static [&'static str] = &[#(#field_name_strs, )*];
                    fn read(buf: &[u8]) -> Result<(Self, &[u8]), ::streamhouse::Error> {
                        #(let (#field_names, buf) = <#field_types as ::streamhouse::Column>::read_value(buf)?;)*
                        Ok((#name { #(#field_names),* }, buf))
                    }
                    
                fn write(&self, buf: &mut impl ::streamhouse::WriteRowBinary) -> Result<(), ::streamhouse::Error> {
                    use ::streamhouse::Column;
                    #(self.#field_names.write_value(buf)?;)*
                    Ok(())                    
                }
                }
            }]
            .into_iter(),
        );
    }
}

