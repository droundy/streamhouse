use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields};

#[proc_macro_derive(Row)]
pub fn derive_row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        ident,
        data,
        attrs: _,
        ..
    } = parse_macro_input!(input);

    let s = RowStruct::parse(&ident, &data);

    let to_impl = RowImpl::to_event(s.name, s.field_names);

    quote!(#to_impl).into()
}

struct RowStruct {
    name: Ident,
    field_names: Vec<Ident>,
    field_types: Vec<Ident>,
}

impl RowStruct {
    fn parse(name: &Ident, data: &Data) -> Self {
        let name = name.clone();

        let fields = match data {
            syn::Data::Struct(DataStruct {
                fields: Fields::Named(fields),
                ..
            }) => fields,
            _ => panic!("Row supports only named struct"),
        };

        EventTypeStruct {
            name,
            field_names: fields.named.iter().map(|f| f.ident.clone()).collect(),
            field_types: fields.named.iter().map(|f| f.typename.clone()).collect(),
        }
    }
}

impl ToTokens for RowStruct {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let name = &self.name;
        let variant = &self.variant;
        let typename_str = Literal::string(&name.to_string());

        let field_names = self
            .field_names
            .iter()
            .filter(|name| !BASE_EVENT_FIELDS.contains(&name.as_str()));
        let fields_map = gen_field_map(&event, field_names);

        let field_types = &self.field_types;
        let field_types2 = &self.field_types;
        let field_types3 = &self.field_types;

        tokens.extend(
            [quote! {
                impl ::streamhouse::Row for #name {
                    const TYPES: &'static [ColumnType] = &[#(#field_types::TYPE, )*];
                    fn read(buf: &mut impl RowBinary) -> Result<Self, Error> {
                        Ok(#name {
                            #(#field_names: <#field_types2 as streamhouse::Column>::read()?.await),*
                        })
                    }
                }
            }]
            .into_iter(),
        );
    }
}
