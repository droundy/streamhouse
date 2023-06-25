use proc_macro2::{Ident, TokenStream};
use quote::{quote, ToTokens};
use syn::{ Data, DataStruct, Fields, Type};

pub(crate) struct ColumnStruct {
    name: Ident,
    field_type: Type,
}

impl ColumnStruct {
    pub fn parse(name: &Ident, data: &Data) -> Self {
        let name = name.clone();

        let fields = match data {
            syn::Data::Struct(DataStruct {
                fields: Fields::Unnamed(fields),
                ..
            }) => fields.unnamed.iter().map(|f| f.ty.clone()).collect::<Vec<_>>(),
            _ => panic!("Column supports only newtype struct"),
        };
        if fields.len() != 1 {
            panic!("Column supports only newtype struct");
        }

        ColumnStruct {
            name,
            field_type: fields.into_iter().next().unwrap(),
        }
    }
}

impl ToTokens for ColumnStruct {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let name = &self.name;
        let typename = &self.field_type;

        tokens.extend(
            [quote! {
                impl ::streamhouse::Column for #name {
                    const TYPE: ::streamhouse::ColumnType = <#typename as ::streamhouse::Column>::TYPE;
                    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), ::streamhouse::Error> {
                        let (value, buf) = <#typename as ::streamhouse::Column>::read_value(buf)?;
                        Ok((#name(value), buf))
                    }
                    
                    fn write_value(&self, buf: &mut impl ::streamhouse::WriteRowBinary) -> Result<(), ::streamhouse::Error> {
                        use ::streamhouse::Column;
                        self.0.write_value(buf)?;
                        Ok(())                    
                    }
                }
            }]
            .into_iter(),
        );
    }
}

