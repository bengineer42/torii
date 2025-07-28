use std::result::Result;
use std::str::FromStr as _;

use dojo_types::packing::ParseError;
use dojo_types::primitive::Primitive;
use dojo_types::schema::{Enum, EnumOption, Member, Struct, Ty};
use dojo_world::contracts::abigen;
use starknet::core::utils::{cairo_short_string_to_felt, parse_cairo_short_string};

fn parse_schema(ty: &abigen::world::Ty) -> Result<Ty, ParseError> {
    match ty {
        abigen::world::Ty::Primitive(primitive) => {
            let ty = parse_cairo_short_string(primitive)?;
            let ty = ty.split("::").last().unwrap();
            let primitive = match Primitive::from_str(ty) {
                Ok(primitive) => primitive,
                Err(_) => return Err(ParseError::invalid_schema()),
            };

            Ok(Ty::Primitive(primitive))
        }
        abigen::world::Ty::Struct(schema) => {
            let name = parse_cairo_short_string(&schema.name)?;

            let children = schema
                .children
                .iter()
                .map(|child| {
                    Ok(Member {
                        name: parse_cairo_short_string(&child.name)?,
                        ty: parse_schema(&child.ty)?,
                        key: child
                            .attrs
                            .contains(&cairo_short_string_to_felt("key").unwrap()),
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;

            Ok(Ty::Struct(Struct { name, children }))
        }
        abigen::world::Ty::Enum(enum_) => {
            let mut name = parse_cairo_short_string(&enum_.name)?;

            let options = enum_
                .children
                .iter()
                .map(|(variant_name, ty)| {
                    // strip "(T)" of the type of the enum variant for now
                    // breaks the db queries
                    // Some(T) => Some
                    let mut variant_name = parse_cairo_short_string(variant_name)?;

                    let ty = parse_schema(ty)?;
                    // generalize this for any generic name?
                    if variant_name.ends_with("(T)") {
                        variant_name = variant_name.trim_end_matches("(T)").to_string();
                        name = name.replace("<T>", format!("<{}>", ty.name()).as_str());
                    }

                    Ok(EnumOption {
                        name: variant_name,
                        ty,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;

            Ok(Ty::Enum(Enum {
                name,
                option: None,
                options,
            }))
        }
        abigen::world::Ty::Tuple(values) => {
            let values = values
                .iter()
                .map(parse_schema)
                .collect::<Result<Vec<_>, ParseError>>()?;

            Ok(Ty::Tuple(values))
        }
        abigen::world::Ty::Array(values) => {
            let values = values
                .iter()
                .map(parse_schema)
                .collect::<Result<Vec<_>, ParseError>>()?;

            Ok(Ty::Array(values))
        }
        abigen::world::Ty::ByteArray => Ok(Ty::ByteArray("".to_string())),
    }
}

pub fn parse_struct_to_schema_with_namespace(
    schema: &abigen::world::Struct,
    name: &String,
    namespace: &String,
) -> Result<Ty, ParseError> {
    let name = format!("{}-{}", namespace, name);
    let children = schema
        .children
        .iter()
        .map(|child| {
            Ok(Member {
                name: parse_cairo_short_string(&child.name)?,
                ty: parse_schema(&child.ty)?,
                key: child
                    .attrs
                    .contains(&cairo_short_string_to_felt("key").unwrap()),
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(Ty::Struct(Struct { name, children }))
}
