use super::errors::CoralError;
use actix_web::Result;
use arrow_schema::{Field, Fields, Schema, SchemaRef, TimeUnit};
use byteorder::{LittleEndian, WriteBytesExt};
use sqlparser::ast::ExactNumberInfo::{Precision, PrecisionAndScale};
use sqlparser::ast::{
    ArrayElemTypeDef, ColumnOption, Expr, ObjectName, SqlOption, Statement, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

pub fn ddl_to_arrow_schema(ddl: &str) -> Result<SchemaRef, CoralError> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, ddl)?;
    if ast.len() != 1 {
        return Err(CoralError::SqlParseError(
            "Expected a single CREATE TABLE statement".to_string(),
        ));
    }

    let stmt = &ast[0];

    match stmt {
        Statement::CreateTable(create_table) => {
            let mut fields = Vec::new();
            for column in &create_table.columns {
                let data_type =
                    sql_type_to_arrow_type(&column.data_type, &column.name.to_string())?;
                fields.push(Field::new(
                    column.name.value.clone(),
                    data_type,
                    column.options.iter().any(|option| {
                        if let ColumnOption::NotNull = option.option {
                            false
                        } else {
                            true
                        }
                    }),
                ));
            }
            let schema = Schema::new(fields);
            let mut metadata = get_metadata_from_with_options(&create_table.with_options);
            handle_segement_id_info(&mut metadata, schema.fields());
            let ObjectName(names) = &create_table.name;
            metadata.insert("table name".to_string(), names[0].value.clone());
            Ok(Arc::new(schema.with_metadata(metadata)))
        }
        _ => Err(CoralError::SqlParseError(
            "Expected a single CREATE TABLE statement".to_string(),
        )),
    }
}

/// Parses metadata from WITH options
fn get_metadata_from_with_options(with_options: &Vec<SqlOption>) -> HashMap<String, String> {
    with_options
        .iter()
        .filter_map(|option| {
            if let Expr::Value(v) = &option.value {
                if let Value::SingleQuotedString(s) = v {
                    let key = option.name.value.clone();
                    let value = s.clone();
                    return Some((key, value));
                }
            }
            None
        })
        .collect::<HashMap<_, _>>()
}

fn conver_to_index_byte_array_string(column_names: &String, fields: &Fields) -> String {
    let indices = column_names
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|s| s.trim())
        .filter_map(|segment_col| fields.iter().position(|col| col.name() == segment_col))
        .collect::<Vec<_>>();

    let mut wtr: Vec<u8> = Vec::with_capacity(indices.len() * std::mem::size_of::<i32>());
    for &idx in &indices {
        wtr.write_i32::<LittleEndian>(idx as i32).unwrap();
    }

    unsafe { String::from_utf8_unchecked(wtr) }
}

pub fn handle_segement_id_info(metadata: &mut HashMap<String, String>, fields: &Fields) {
    if let Some(segment_id_info) = metadata.get("segment_id_info") {
        let segment_ids = conver_to_index_byte_array_string(&segment_id_info, fields);
        metadata.insert("segment_id_info".to_string(), segment_ids);
    }
}

fn sql_type_to_arrow_type(
    sql_type: &sqlparser::ast::DataType,
    name: &String,
) -> Result<arrow_schema::DataType, CoralError> {
    match sql_type {
        sqlparser::ast::DataType::TinyInt(_) => Ok(arrow_schema::DataType::Int8),
        sqlparser::ast::DataType::UnsignedTinyInt(_) => Ok(arrow_schema::DataType::UInt8),
        sqlparser::ast::DataType::Int(_) => Ok(arrow_schema::DataType::Int32),
        sqlparser::ast::DataType::Integer(_) => Ok(arrow_schema::DataType::Int32),
        sqlparser::ast::DataType::UnsignedInt(_) => Ok(arrow_schema::DataType::UInt32),
        sqlparser::ast::DataType::UnsignedInt4(_) => Ok(arrow_schema::DataType::UInt32),
        sqlparser::ast::DataType::UnsignedInteger(_) => Ok(arrow_schema::DataType::UInt32),
        sqlparser::ast::DataType::BigInt(_) => Ok(arrow_schema::DataType::Int64),
        sqlparser::ast::DataType::UnsignedBigInt(_) => Ok(arrow_schema::DataType::UInt64),
        sqlparser::ast::DataType::UnsignedInt8(_) => Ok(arrow_schema::DataType::UInt64),
        sqlparser::ast::DataType::SmallInt(_) => Ok(arrow_schema::DataType::Int16),
        sqlparser::ast::DataType::UnsignedSmallInt(_) => Ok(arrow_schema::DataType::UInt16),
        sqlparser::ast::DataType::UnsignedInt2(_) => Ok(arrow_schema::DataType::UInt16),
        sqlparser::ast::DataType::Char(_) => Ok(arrow_schema::DataType::Utf8),
        sqlparser::ast::DataType::Varchar(_) => Ok(arrow_schema::DataType::Utf8),
        sqlparser::ast::DataType::Text => Ok(arrow_schema::DataType::Utf8),
        sqlparser::ast::DataType::CharLargeObject(_) => Ok(arrow_schema::DataType::LargeUtf8),
        sqlparser::ast::DataType::CharacterLargeObject(_) => Ok(arrow_schema::DataType::LargeUtf8),
        sqlparser::ast::DataType::Float(_) => Ok(arrow_schema::DataType::Float32),
        sqlparser::ast::DataType::Double => Ok(arrow_schema::DataType::Float64),
        sqlparser::ast::DataType::Decimal(Precision(length)) => {
            Ok(arrow_schema::DataType::Decimal128(length.clone() as u8, 0))
        }
        sqlparser::ast::DataType::Decimal(PrecisionAndScale(length, scale)) => Ok(
            arrow_schema::DataType::Decimal128(length.clone() as u8, scale.clone() as i8),
        ),
        sqlparser::ast::DataType::Boolean => Ok(arrow_schema::DataType::Boolean),
        sqlparser::ast::DataType::Date => Ok(arrow_schema::DataType::Date32),
        sqlparser::ast::DataType::Timestamp(_, _) => Ok(arrow_schema::DataType::Timestamp(
            TimeUnit::Nanosecond,
            None,
        )),
        sqlparser::ast::DataType::Array(item_type_def) => match item_type_def {
            ArrayElemTypeDef::AngleBracket(data_type) => {
                Ok(arrow_schema::DataType::List(Arc::new(Field::new(
                    "item",
                    sql_type_to_arrow_type(data_type.as_ref(), name)?,
                    true,
                ))))
            }
            ArrayElemTypeDef::SquareBracket(data_type, length) => match length {
                Some(len) => Ok(arrow_schema::DataType::FixedSizeList(
                    Arc::new(Field::new(
                        "item",
                        sql_type_to_arrow_type(data_type.as_ref(), name)?,
                        true,
                    )),
                    *len as i32,
                )),
                None => Err(CoralError::SqlParseError(format!(
                    "Fixed list type require explicit length for column={name}, type={sql_type}"
                ))),
            },
            _ => Err(CoralError::SqlParseError(format!(
                "Not supported array/list data type for column={name}, type={sql_type}"
            ))),
        },
        _ => Err(CoralError::SqlParseError(format!(
            "Not supported data type for column={name}, type={sql_type}"
        ))),
    }
}

pub fn arrow_schema_to_ddl(table_name: String, schema: SchemaRef) -> Result<String, CoralError> {
    let mut sql = format!("CREATE TABLE {} (", table_name);
    for (i, field) in schema.fields().iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "{} {}",
            field.name(),
            arrow_data_type_to_sql_data_type(&field.data_type())?
        ));
    }
    sql.push_str(") WITH ( ... write metadata ... );");
    Ok(sql)
}

fn arrow_data_type_to_sql_data_type(
    datatype: &arrow_schema::DataType,
) -> Result<String, CoralError> {
    match datatype {
        arrow_schema::DataType::Boolean => Ok("BOOLEAN".to_string()),
        arrow_schema::DataType::Int8 => Ok("TINYINT".to_string()),
        arrow_schema::DataType::Int16 => Ok("SMALLINT".to_string()),
        arrow_schema::DataType::Int32 => Ok("INT".to_string()),
        arrow_schema::DataType::Int64 => Ok("BIGINT".to_string()),
        arrow_schema::DataType::UInt8 => Ok("TINYINT UNSIGNED".to_string()),
        arrow_schema::DataType::UInt16 => Ok("SMALLINT UNSIGNED".to_string()),
        arrow_schema::DataType::UInt32 => Ok("INT UNSIGNED".to_string()),
        arrow_schema::DataType::UInt64 => Ok("BIGINT UNSIGNED".to_string()),
        arrow_schema::DataType::Float16 => Ok("FLOAT".to_string()),
        arrow_schema::DataType::Float32 => Ok("FLOAT".to_string()),
        arrow_schema::DataType::Float64 => Ok("DOUBLE".to_string()),
        arrow_schema::DataType::Utf8 => Ok("TEXT".to_string()),
        arrow_schema::DataType::LargeUtf8 => Ok("CHAR LARGE OBJECT".to_string()),
        arrow_schema::DataType::Date32 => Ok("DATE".to_string()),
        arrow_schema::DataType::Date64 => Ok("DATETIME".to_string()),
        arrow_schema::DataType::Timestamp(_, _) => Ok("TIMESTAMP".to_string()),
        arrow_schema::DataType::Time32(_) => Ok("TIME".to_string()),
        arrow_schema::DataType::Time64(_) => Ok("TIME".to_string()),
        arrow_schema::DataType::List(field) => Ok(format!(
            "ARRAY<{}>",
            arrow_data_type_to_sql_data_type(field.data_type())?
        )),
        arrow_schema::DataType::FixedSizeList(field, size) => Ok(format!(
            "{}[{}]",
            arrow_data_type_to_sql_data_type(field.data_type())?,
            size
        )),
        arrow_schema::DataType::Decimal128(precision, scale) => {
            Ok(format!("DECIMAL({},{})", precision, scale))
        }
        _ => Err(CoralError::InternalError(
            "Not supported data type".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_ddl_to_arrow_schema_with_nested_types_and_metadata() -> Result<(), CoralError> {
        let ddl = r#"
            CREATE TABLE complex_table (
                id INTEGER,
                name VARCHAR,
                tags ARRAY<VARCHAR>,
                fixed_list FLOAT[3],
                is_active BOOLEAN,
                created_at TIMESTAMP,
                scores ARRAY<FLOAT>,
                description TEXT,
                age TINYINT,
                salary BIGINT,
                height SMALLINT,
                rating DOUBLE NOT NULL,
                birth_date DATE,
                stored_timestamp TIMESTAMP,
                unsigned_age TINYINT UNSIGNED,
                unsigned_salary BIGINT UNSIGNED,
                unsigned_height SMALLINT UNSIGNED,
                unsigned_int INTEGER UNSIGNED,
                large_string CHAR LARGE OBJECT
            )
            WITH (
                description = 'A complex table with nested types',
                "created_at" = '2023-06-01',
                "version" = '1.0',
                some_other_option = 'value',
                segment_id_info = '[id, age, rating]'
            )
        "#;

        let schema = ddl_to_arrow_schema(ddl)?;

        // Check fields
        assert_eq!(schema.fields().len(), 19);

        // Check INTEGER type
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int32);

        // Check VARCHAR type
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Utf8);

        // Check ARRAY type for tags
        assert_eq!(schema.field(2).name(), "tags");
        if let arrow_schema::DataType::List(field) = schema.field(2).data_type() {
            assert_eq!(field.data_type(), &arrow_schema::DataType::Utf8);
        } else {
            panic!("Expected List type for 'tags'");
        }

        // Check FIXED SIZE LIST type for fixed_list
        assert_eq!(schema.field(3).name(), "fixed_list");
        if let arrow_schema::DataType::FixedSizeList(field, size) = schema.field(3).data_type() {
            assert_eq!(field.data_type(), &arrow_schema::DataType::Float32);
            assert_eq!(size, &3);
        } else {
            panic!("Expected FixedSizeList type for 'fixed_list'");
        }

        // Check BOOLEAN type
        assert_eq!(schema.field(4).name(), "is_active");
        assert_eq!(
            schema.field(4).data_type(),
            &arrow_schema::DataType::Boolean
        );

        // Check TIMESTAMP type
        assert_eq!(schema.field(5).name(), "created_at");
        assert_eq!(
            schema.field(5).data_type(),
            &arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        // Check ARRAY type for scores
        assert_eq!(schema.field(6).name(), "scores");
        if let arrow_schema::DataType::List(field) = schema.field(6).data_type() {
            assert_eq!(field.data_type(), &arrow_schema::DataType::Float32);
        } else {
            panic!("Expected List type for 'scores'");
        }

        // Check TEXT type
        assert_eq!(schema.field(7).name(), "description");
        assert_eq!(schema.field(7).data_type(), &arrow_schema::DataType::Utf8);

        // Check TINYINT type
        assert_eq!(schema.field(8).name(), "age");
        assert_eq!(schema.field(8).data_type(), &arrow_schema::DataType::Int8);

        // Check BIGINT type
        assert_eq!(schema.field(9).name(), "salary");
        assert_eq!(schema.field(9).data_type(), &arrow_schema::DataType::Int64);

        // Check SMALLINT type
        assert_eq!(schema.field(10).name(), "height");
        assert_eq!(schema.field(10).data_type(), &arrow_schema::DataType::Int16);

        // Check DOUBLE type
        assert_eq!(schema.field(11).name(), "rating");
        assert_eq!(
            schema.field(11).data_type(),
            &arrow_schema::DataType::Float64
        );
        assert!(!schema.field(11).is_nullable());

        // Check DATE type
        assert_eq!(schema.field(12).name(), "birth_date");
        assert_eq!(
            schema.field(12).data_type(),
            &arrow_schema::DataType::Date32
        );

        // Check TIMESTAMP type
        assert_eq!(schema.field(13).name(), "stored_timestamp");
        assert_eq!(
            schema.field(13).data_type(),
            &arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        // Check UNSIGNED TINYINT type
        assert_eq!(schema.field(14).name(), "unsigned_age");
        assert_eq!(schema.field(14).data_type(), &arrow_schema::DataType::UInt8);

        // Check UNSIGNED BIGINT type
        assert_eq!(schema.field(15).name(), "unsigned_salary");
        assert_eq!(
            schema.field(15).data_type(),
            &arrow_schema::DataType::UInt64
        );

        // Check UNSIGNED SMALLINT type
        assert_eq!(schema.field(16).name(), "unsigned_height");
        assert_eq!(
            schema.field(16).data_type(),
            &arrow_schema::DataType::UInt16
        );

        // Check UNSIGNED INTEGER type
        assert_eq!(schema.field(17).name(), "unsigned_int");
        assert_eq!(
            schema.field(17).data_type(),
            &arrow_schema::DataType::UInt32
        );

        // Check CHARACTER LARGE OBJECT type
        assert_eq!(schema.field(18).name(), "large_string");
        assert_eq!(
            schema.field(18).data_type(),
            &arrow_schema::DataType::LargeUtf8
        );

        // Check metadata
        let schema_metadata = schema.metadata();
        assert_eq!(
            schema_metadata.get("description"),
            Some(&"A complex table with nested types".to_string())
        );
        assert_eq!(
            schema_metadata.get("created_at"),
            Some(&"2023-06-01".to_string())
        );
        assert_eq!(schema_metadata.get("version"), Some(&"1.0".to_string()));
        assert_eq!(
            schema_metadata.get("some_other_option"),
            Some(&"value".to_string())
        );

        let segment_id_info = schema_metadata.get("segment_id_info").and_then(|value| {
            let bytes = value.as_bytes();
            let mut vec: Vec<i32> = Vec::new();
            for chunk in bytes.chunks(4) {
                vec.push(i32::from_le_bytes(chunk.try_into().unwrap_or_default()));
            }
            Some(vec)
        });
        assert_eq!(segment_id_info.unwrap(), vec![0, 8, 11]);

        Ok(())
    }

    #[test]
    fn test_ddl_to_arrow_schema_not_supported_type() -> Result<(), CoralError> {
        // === check fixed size list without length
        let ddl = r#"
            CREATE TABLE complex_table (
                not_supported INT[]
            )
        "#;
        let parsed = ddl_to_arrow_schema(ddl);

        assert!(matches!(parsed, Err(CoralError::SqlParseError(_))));
        if let Err(CoralError::SqlParseError(msg)) = parsed {
            assert_eq!(
                "Fixed list type require explicit length for column=not_supported, type=INT[]"
                    .to_string(),
                msg
            );
        } else {
            panic!("Expected a SqlParseError");
        }

        // === check unsupported
        let ddl = r#"
            CREATE TABLE complex_table (
                not_supported JSON
            )
        "#;
        let parsed = ddl_to_arrow_schema(ddl);

        assert!(matches!(parsed, Err(CoralError::SqlParseError(_))));
        if let Err(CoralError::SqlParseError(msg)) = parsed {
            assert_eq!(
                "Not supported data type for column=not_supported, type=JSON".to_string(),
                msg
            );
        } else {
            panic!("Expected a SqlParseError");
        }
        Ok(())
    }

    #[test]
    fn test_arrow_schema_to_ddl() -> Result<(), CoralError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow_schema::DataType::Int32, false),
            Field::new("name", arrow_schema::DataType::Utf8, false),
            Field::new("title", arrow_schema::DataType::LargeUtf8, false),
            Field::new("is_active", arrow_schema::DataType::Boolean, false),
            Field::new(
                "created_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let ddl = arrow_schema_to_ddl("test_table".to_string(), schema)?;

        assert_eq!(ddl, "CREATE TABLE test_table (id INT, name TEXT, title CHAR LARGE OBJECT, is_active BOOLEAN, created_at TIMESTAMP) WITH ( ... write metadata ... );");
        Ok(())
    }
}
