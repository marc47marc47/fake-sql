use rand::prelude::*;
use std::fmt;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::Local;

#[derive(Debug, Clone)]
enum ColumnType {
    String,
    Number,
    Date,
    DateTime,
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    col_type: ColumnType,
}

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: Vec<Column>,
}

impl Table {
    fn new(name: &str, custom_columns: Vec<Column>) -> Self {
        let mut columns = custom_columns;
        columns.push(Column {
            name: "creation_date".to_string(),
            col_type: ColumnType::DateTime,
        });
        columns.push(Column {
            name: "last_updated_date".to_string(),
            col_type: ColumnType::DateTime,
        });
        columns.push(Column {
            name: "created_by".to_string(),
            col_type: ColumnType::String,
        });
        columns.push(Column {
            name: "last_updated_by".to_string(),
            col_type: ColumnType::String,
        });
        Table {
            name: name.to_string(),
            columns,
        }
    }
}

struct TableSchema;

impl TableSchema {
    fn orders() -> Table {
        Table::new("ord_orders", vec![
            Column { name: "order_id".to_string(), col_type: ColumnType::Number },
            Column { name: "customer_id".to_string(), col_type: ColumnType::Number },
            Column { name: "order_date".to_string(), col_type: ColumnType::Date },
        ])
    }

    fn customers() -> Table {
        Table::new("pub_customers", vec![
            Column { name: "customer_id".to_string(), col_type: ColumnType::Number },
            Column { name: "name".to_string(), col_type: ColumnType::String },
            Column { name: "email".to_string(), col_type: ColumnType::String },
            Column { name: "phone".to_string(), col_type: ColumnType::String },
        ])
    }

    fn products() -> Table {
        Table::new("pub_products", vec![
            Column { name: "product_id".to_string(), col_type: ColumnType::Number },
            Column { name: "product_name".to_string(), col_type: ColumnType::String },
            Column { name: "price".to_string(), col_type: ColumnType::Number },
        ])
    }

    fn order_details() -> Table {
        Table::new("ord_order_details", vec![
            Column { name: "order_id".to_string(), col_type: ColumnType::Number },
            Column { name: "product_id".to_string(), col_type: ColumnType::Number },
            Column { name: "quantity".to_string(), col_type: ColumnType::Number },
            Column { name: "price".to_string(), col_type: ColumnType::Number },
        ])
    }

    fn tmp_order_agg() -> Table {
        Table::new("tmp_order_agg", vec![
            Column { name: "order_id".to_string(), col_type: ColumnType::Number },
            Column { name: "total_amount".to_string(), col_type: ColumnType::Number },
            Column { name: "order_date".to_string(), col_type: ColumnType::Date },
        ])
    }
}

#[derive(Debug, Clone)]
enum SqlStatement {
    CreateTable(Table),
    DropTable(String),
    AlterTable(String),
    Insert(String, Vec<String>),
    Delete(String, String, i32),
    Update(String, Vec<String>, String, i32),
    Select(String, Vec<String>, Vec<String>),
}

impl fmt::Display for SqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let current_date = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        match self {
            SqlStatement::CreateTable(table) => write!(
                f,
                "create table {} ({})",
                table.name,
                table
                    .columns
                    .iter()
                    .map(|col| format!("{} {}", col.name, match col.col_type {
                        ColumnType::String => "VARCHAR2(100)",
                        ColumnType::Number => "NUMBER",
                        ColumnType::Date => "DATE",
                        ColumnType::DateTime => "TIMESTAMP",
                    }))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            SqlStatement::DropTable(name) => write!(f, "drop table {}", name),
            SqlStatement::AlterTable(name) => write!(f, "alter table {} add column new_column VARCHAR2(50)", name),
            SqlStatement::Insert(table_name, columns) => write!(
                f,
                "insert into {} ({}) values (1, TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS'), 'John', 'Jane')",
                table_name,
                columns.join(", "),
                current_date,
                current_date
            ),
            SqlStatement::Delete(table_name, id_column, id_value) => write!(
                f,
                "delete from {} where {} = {}",
                table_name,
                id_column,
                id_value
            ),
            SqlStatement::Update(table_name, updates, id_column, id_value) => write!(
                f,
                "update {} set {} where {} = {}",
                table_name,
                updates.join(", "),
                id_column,
                id_value
            ),
            SqlStatement::Select(table_name, columns, conditions) => write!(
                f,
                "select {} from {} where {}",
                columns.join(", "),
                table_name,
                conditions.join(" and ")
            ),
        }
    }
}

fn generate_sql_statement() -> SqlStatement {
    let mut rng = rand::thread_rng();
    let weight_distribution = [10, 10, 10, 20, 20, 20, 10];
    let choices = [
        SqlStatement::CreateTable(TableSchema::tmp_order_agg()),
        SqlStatement::DropTable("tmp_order_agg".to_string()),
        SqlStatement::AlterTable("tmp_order_agg".to_string()),
        SqlStatement::Insert(
            "ord_orders".to_string(),
            TableSchema::orders().columns.iter().map(|c| c.name.clone()).collect(),
        ),
        SqlStatement::Insert(
            "pub_customers".to_string(),
            TableSchema::customers().columns.iter().map(|c| c.name.clone()).collect(),
        ),
        SqlStatement::Insert(
            "pub_products".to_string(),
            TableSchema::products().columns.iter().map(|c| c.name.clone()).collect(),
        ),
        SqlStatement::Insert(
            "ord_order_details".to_string(),
            TableSchema::order_details().columns.iter().map(|c| c.name.clone()).collect(),
        ),
        SqlStatement::Delete("ord_orders".to_string(), "order_id".to_string(), rng.gen_range(3333..=99999)),
        SqlStatement::Delete("pub_customers".to_string(), "customer_id".to_string(), rng.gen_range(3333..=99999)),
        SqlStatement::Delete("pub_products".to_string(), "product_id".to_string(), rng.gen_range(3333..=99999)),
        SqlStatement::Delete("ord_order_details".to_string(), "order_id".to_string(), rng.gen_range(3333..=99999)),
        SqlStatement::Update(
            "ord_orders".to_string(),
            vec!["last_updated_date = TO_TIMESTAMP('2023-11-12 12:00:00', 'YYYY-MM-DD HH24:MI:SS')", "last_updated_by = 'John'"].iter().map(|&s| s.to_string()).collect(),
            "order_id".to_string(),
            rng.gen_range(3333..=99999),
        ),
        SqlStatement::Update(
            "pub_customers".to_string(),
            vec!["last_updated_date = TO_TIMESTAMP('2023-11-12 12:00:00', 'YYYY-MM-DD HH24:MI:SS')", "last_updated_by = 'John'"].iter().map(|&s| s.to_string()).collect(),
            "customer_id".to_string(),
            rng.gen_range(3333..=99999),
        ),
        SqlStatement::Update(
            "pub_products".to_string(),
            vec!["last_updated_date = TO_TIMESTAMP('2023-11-12 12:00:00', 'YYYY-MM-DD HH24:MI:SS')", "last_updated_by = 'John'"].iter().map(|&s| s.to_string()).collect(),
            "product_id".to_string(),
            rng.gen_range(3333..=99999),
        ),
        SqlStatement::Update(
            "ord_order_details".to_string(),
            vec!["last_updated_date = TO_TIMESTAMP('2023-11-12 12:00:00', 'YYYY-MM-DD HH24:MI:SS')", "last_updated_by = 'John'"].iter().map(|&s| s.to_string()).collect(),
            "order_id".to_string(),
            rng.gen_range(3333..=99999),
        ),
        SqlStatement::Select(
            "ord_orders".to_string(),
            {
                let selected_columns = vec!["order_id", "customer_id", "order_date", "created_by", "creation_date", "last_updated_by", "last_updated_date"];
                let num_columns = rng.gen_range(1..=3);
                selected_columns.choose_multiple(&mut rng, num_columns).cloned().map(String::from).collect()
            },
            {
                let conditions_pool = vec![
                    format!("order_id > {}", rng.gen_range(3333..=99999)),
                    format!("customer_id <= {}", rng.gen_range(3333..=99999)),
                    "status in ('PENDING', 'COMPLETED')".to_string(),
                    format!("creation_date between TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS') and TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS')", Local::now().format("%Y-%m-%d 00:00:00"), Local::now().format("%Y-%m-%d %H:%M:%S")),
                    "created_by in ('John', 'Jane', 'Alice')".to_string(),
                ];
                let num_conditions = rng.gen_range(1..=5);
                conditions_pool.choose_multiple(&mut rng, num_conditions).cloned().collect()
            },
        ),
    ];
    choices
        .choose_weighted(&mut rng, |item| match item {
            SqlStatement::CreateTable(_) => weight_distribution[0],
            SqlStatement::DropTable(_) => weight_distribution[1],
            SqlStatement::AlterTable(_) => weight_distribution[2],
            SqlStatement::Insert(_, _) => weight_distribution[3],
            SqlStatement::Delete(_, _, _) => weight_distribution[4],
            SqlStatement::Update(_, _, _, _) => weight_distribution[5],
            SqlStatement::Select(_, _, _) => weight_distribution[6],
        })
        .unwrap()
        .clone()
}

fn main() {
    let start = SystemTime::now();
    let datetime = start.duration_since(UNIX_EPOCH).unwrap();
    let timestamp = chrono::NaiveDateTime::from_timestamp(datetime.as_secs() as i64, 0);
    let file_name = format!("sql_logs_{}.log", timestamp.format("%Y_%m_%d_%H_%M"));
    let mut file = File::create(&file_name).expect("Unable to create file");

    for _ in 0..1000 {
        let sql_statement = generate_sql_statement();
        writeln!(file, "{}", sql_statement).expect("Unable to write data");
    }
    println!("SQL logs written to: {}", file_name);
}

