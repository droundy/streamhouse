use quote::quote;
use syn::{parse_macro_input, DeriveInput};

mod column;
mod row;

#[proc_macro_derive(Column)]
pub fn derive_column(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        ident,
        data,
        attrs: _,
        ..
    } = parse_macro_input!(input);

    let s = column::ColumnStruct::parse(&ident, &data);

    quote!(#s).into()
}

#[proc_macro_derive(Row)]
pub fn derive_row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        ident,
        data,
        attrs: _,
        ..
    } = parse_macro_input!(input);

    let s = row::RowStruct::parse(&ident, &data);

    quote!(#s).into()
}
