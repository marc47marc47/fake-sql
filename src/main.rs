use rand::prelude::*;
use std::fmt;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

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
    fn new(name: &str) -> Self {
        let mut columns = vec![
            Column {
                name: "creation_date".to_string(),
                col_type: ColumnType::DateTime,
            },
            Column {
                name: "last_updated_date".to_string(),
                col_type: ColumnType::DateTime,
            },
            Column {
                name: "created_by".to_string(),
                col_type: ColumnType::String,
            },
            Column {
                name: "last_updated_by".to_string(),
                col_type: ColumnType::String,
            },
        ];
        columns.push(Column {
            name: "id".to_string(),
            col_type: ColumnType::Number,
        });
        Table {
            name: name.to_string(),
            columns,
        }
    }
}

#[derive(Debug, Clone)]
enum SqlStatement {
    CreateTable(Table),
    DropTable(String),
    AlterTable(String),
    Insert(String),
    Delete(String),
    Update(String),
    Select(String),
}

impl fmt::Display for SqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            SqlStatement::Insert(table_name) => write!(f, "insert into {} (id, creation_date, last_updated_date, created_by, last_updated_by) values (1, sysdate, sysdate, 'User1', 'User2')", table_name),
            SqlStatement::Delete(table_name) => write!(f, "delete from {} where id = 1", table_name),
            SqlStatement::Update(table_name) => write!(f, "update {} set last_updated_date = sysdate, last_updated_by = 'User3' where id = 1", table_name),
            SqlStatement::Select(table_name) => write!(f, "select * from {} where creation_date between sysdate - 30 and sysdate", table_name),
        }
    }
}

fn generate_sql_statement() -> SqlStatement {
    let mut rng = rand::thread_rng();
    let weight_distribution = [3, 3, 4, 20, 5, 10, 55];
    let choices = [
        SqlStatement::CreateTable(Table::new("my_table")),
        SqlStatement::DropTable("my_table".to_string()),
        SqlStatement::AlterTable("my_table".to_string()),
        SqlStatement::Insert("my_table".to_string()),
        SqlStatement::Delete("my_table".to_string()),
        SqlStatement::Update("my_table".to_string()),
        SqlStatement::Select("my_table".to_string()),
    ];
    choices
        .choose_weighted(&mut rng, |item| match item {
            SqlStatement::CreateTable(_) => weight_distribution[0],
            SqlStatement::DropTable(_) => weight_distribution[1],
            SqlStatement::AlterTable(_) => weight_distribution[2],
            SqlStatement::Insert(_) => weight_distribution[3],
            SqlStatement::Delete(_) => weight_distribution[4],
            SqlStatement::Update(_) => weight_distribution[5],
            SqlStatement::Select(_) => weight_distribution[6],
        })
        .unwrap()
        .clone()
}

fn main() {
    let mut rng = rand::thread_rng();
    let start = SystemTime::now();
    let timestamp = start.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let file_name = format!("sql_logs_{}.tsv", timestamp);
    let mut file = File::create(&file_name).expect("Unable to create file");

    for _ in 0..1000 {
        let sql_statement = generate_sql_statement();
        writeln!(file, "{}", sql_statement).expect("Unable to write data");
    }
    println!("SQL logs written to: {}", file_name);
}

