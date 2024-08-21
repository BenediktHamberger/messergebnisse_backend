use chrono::{NaiveDate, NaiveDateTime};
use deadpool_tiberius::Client;
use http::StatusCode;
use serde_json::Value;

pub fn map_columns(rows: Vec<tiberius::Row>, res: &mut Vec<Vec<Value>>) {
    for row in rows {
        let mut temp_row = vec![];
        for col_idx in 0..row.len() {
            let col = match row.columns()[col_idx].column_type() {
                tiberius::ColumnType::Null => Value::Null,
                tiberius::ColumnType::Bit => Value::Bool(row.get(col_idx).unwrap_or(false)),
                tiberius::ColumnType::Int1 => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Int2 => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Int4 => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Int8 => Value::from(row.get::<i64, _>(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Datetime4 => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Float4 => Value::from(row.get(col_idx).unwrap_or(0.0)),
                tiberius::ColumnType::Float8 => Value::from(row.get(col_idx).unwrap_or(0.0)),
                tiberius::ColumnType::Money => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Datetime => Value::from(
                    row.get::<NaiveDateTime, _>(col_idx)
                        .unwrap_or_default()
                        .to_string(),
                ),
                tiberius::ColumnType::Money4 => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Guid => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Intn => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Bitn => {
                    Value::from(row.get::<bool, _>(col_idx).unwrap_or(false))
                }
                tiberius::ColumnType::Decimaln => Value::from(row.get(col_idx).unwrap_or(0.0)),
                tiberius::ColumnType::Numericn => Value::from(
                    row.get(col_idx)
                        .unwrap_or(tiberius::numeric::Numeric::new_with_scale(0, 1))
                        .value() as i64,
                ),
                tiberius::ColumnType::Floatn => {
                    Value::from(row.get::<f32, _>(col_idx).unwrap_or(0.0))
                }
                tiberius::ColumnType::Datetimen => Value::from(
                    row.get::<NaiveDateTime, _>(col_idx)
                        .unwrap_or_default()
                        .to_string(),
                ),
                tiberius::ColumnType::Daten => Value::from(
                    row.get::<NaiveDate, _>(col_idx)
                        .unwrap_or_default()
                        .to_string(),
                ),
                tiberius::ColumnType::Timen => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Datetime2 => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::DatetimeOffsetn => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::BigVarBin => Value::from(row.get(col_idx).unwrap_or("")),
                tiberius::ColumnType::BigVarChar => Value::from(row.get(col_idx).unwrap_or("")),
                tiberius::ColumnType::BigBinary => Value::from(row.get(col_idx).unwrap_or("")),
                tiberius::ColumnType::BigChar => Value::from(row.get(col_idx).unwrap_or("")),
                tiberius::ColumnType::NVarchar => Value::from(row.get(col_idx).unwrap_or("")),
                tiberius::ColumnType::NChar => Value::from(row.get(col_idx).unwrap_or("")),
                tiberius::ColumnType::Xml => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Udt => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Text => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::Image => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::NText => Value::from(row.get(col_idx).unwrap_or(0)),
                tiberius::ColumnType::SSVariant => Value::from(row.get(col_idx).unwrap_or(0)),
                // if let Some(col) = Value::from(row.get(col_idx).unwrap_or(0)) {
                //     temp_row.push(serde_json::to_value(&col).unwrap_or(Value::Null));
                // }
            };
            temp_row.push(col);
        }
        res.push(temp_row);
    }
}

pub async fn actual_query<'a>(
    query: String,
    // mut conn: deadpool_tiberius::deadpool::managed::Object<deadpool_tiberius::Manager>,
    conn: &'a mut Client,
    res: &mut Vec<Vec<Value>>,
) -> Result<(), (StatusCode, String)> {
    let select = tiberius::Query::new(&query);

    // #[cfg(debug_assertions)]
    // println!("{}", query);

    #[cfg(debug_assertions)]
    let mut streams = select
        .query(conn)
        .await
        .map_err(|_| (StatusCode::BAD_REQUEST, query))?;

    #[cfg(not(debug_assertions))]
    let mut streams = select
        .query(conn)
        .await
        .map_err(|x| (StatusCode::BAD_REQUEST, x.to_string()))?;

    let columns = streams
        .columns()
        .await
        .map_err(|x| (StatusCode::INTERNAL_SERVER_ERROR, x.to_string()))?;

    if let Some(c) = columns {
        res.push(
            c.iter()
                .map(|x| Value::String(x.name().into()))
                .collect::<Vec<Value>>(),
        )
    }

    let rows = streams
        .into_first_result()
        .await
        .map_err(|x| (StatusCode::INTERNAL_SERVER_ERROR, x.to_string()))?;

    map_columns(rows, res);

    Ok(())
}

pub enum Filter {
    EQ(String, String),
    Contains(String, String),
    NotContains(String, String),
    Between(String, String, String),
    GT(String, String),
    LT(String, String),
    Empty(String),
    NotEmpty(String),
}

impl From<crate::InpFilter> for Filter {
    fn from(inp: crate::InpFilter) -> Self {
        match inp.operator.as_str() {
            "Contains" => Filter::Contains(inp.id, inp.value.unwrap_or_default()),
            "NotContains" => Filter::NotContains(inp.id, inp.value.unwrap_or_default()),
            "Between" => Filter::Between(
                inp.id,
                inp.lower.unwrap_or_default(),
                inp.upper.unwrap_or_default(),
            ),
            "GT" => Filter::GT(inp.id, inp.value.unwrap_or_default()),
            "LT" => Filter::LT(inp.id, inp.value.unwrap_or_default()),
            "Empty" => Filter::Empty(inp.id),
            "NotEmpty" => Filter::NotEmpty(inp.id),
            _ => Filter::EQ(inp.id, inp.value.unwrap_or_default()),
        }
    }
}

pub trait GetId {
    fn get_id(&self) -> String;
}

impl GetId for Filter {
    fn get_id(&self) -> String {
        match &self {
            Filter::EQ(id, _value) => id.clone(),
            Filter::Empty(id) => id.clone(),
            Filter::NotEmpty(id) => id.clone(),
            Filter::Contains(id, _value) => id.clone(),
            Filter::NotContains(id, _value) => id.clone(),
            Filter::Between(id, _lower, _upper) => id.clone(),
            Filter::LT(id, _lower) => id.clone(),
            Filter::GT(id, _upper) => id.clone(),
        }
    }
}

pub enum SqlType {
    SqlString,
    SqlNumber,
    SqlDate,
}

pub fn match_operator(f: Filter, sql_type: SqlType, vec_filters: &mut Vec<String>) {
    match sql_type {
        SqlType::SqlString => match f {
            Filter::EQ(id, value) => {
                vec_filters.push(format!("{} = '{}'", id, value));
            }
            Filter::Empty(id) => {
                vec_filters.push(format!("ISNULL({}, '') = ''", id));
            }
            Filter::NotEmpty(id) => {
                vec_filters.push(format!("ISNULL({}, '') != ''", id));
            }
            Filter::Contains(id, value) => {
                vec_filters.push(format!("{} LIKE '%{}%'", id, value));
            }
            Filter::NotContains(id, value) => {
                vec_filters.push(format!("{} NOT LIKE '%{}%'", id, value));
            }
            Filter::Between(id, lower, upper) => {
                vec_filters.push(format!("({} > '{}' AND {} < '{}')", id, lower, id, upper));
            }
            Filter::LT(id, lower) => {
                vec_filters.push(format!("({} < '{}')", id, lower));
            }
            Filter::GT(id, upper) => {
                vec_filters.push(format!("({} > '{}')", id, upper));
            }
        },
        SqlType::SqlNumber => match f {
            Filter::EQ(id, value) => {
                vec_filters.push(format!("{} = {}", id, value));
            }
            Filter::Empty(id) => {
                vec_filters.push(format!("ISNULL({}, '') = ''", id));
            }
            Filter::NotEmpty(id) => {
                vec_filters.push(format!("ISNULL({}, '') != ''", id));
            }
            Filter::Contains(id, value) => {
                vec_filters.push(format!("{} LIKE %{}%", id, value));
            }
            Filter::NotContains(id, value) => {
                vec_filters.push(format!("{} NOT LIKE %{}%", id, value));
            }
            Filter::Between(id, lower, upper) => {
                vec_filters.push(format!("({} > {} AND {} < {})", id, lower, id, upper));
            }
            Filter::LT(id, lower) => {
                vec_filters.push(format!("({} > {})", id, lower));
            }
            Filter::GT(id, upper) => {
                vec_filters.push(format!("({} < {})", id, upper));
            }
        },
        SqlType::SqlDate => match f {
            Filter::EQ(id, value) => {
                vec_filters.push(format!("{} = '{}'", id, value));
            }
            Filter::Empty(id) => {
                vec_filters.push(format!("ISNULL({}, '') = ''", id));
            }
            Filter::NotEmpty(id) => {
                vec_filters.push(format!("ISNULL({}, '') != ''", id));
            }
            Filter::Contains(id, value) => {
                vec_filters.push(format!("{} LIKE %{}%", id, value));
            }
            Filter::NotContains(id, value) => {
                vec_filters.push(format!("{} NOT LIKE %{}%", id, value));
            }
            Filter::Between(id, lower, upper) => {
                vec_filters.push(format!("({} > '{}' AND {} < '{}')", id, lower, id, upper));
            }
            Filter::LT(id, lower) => {
                vec_filters.push(format!("({} < '{}')", id, lower));
            }
            Filter::GT(id, upper) => {
                vec_filters.push(format!("({} > '{}')", id, upper));
            }
        },
    }
}
