use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::OpenOptions;
use std::io::Write;

//struct Table
// Name: String
// ref_table: String
// ref_column: String


#[derive(Copy, Clone)]
enum SqlType {
    CreateTable,
    AlterTable,
    DropTable,
    Insert,
    Select,
    Update,
    Delete,
}


struct Table {
    name: String,
    ref_table: String,
    ref_column: String,
}

//struct column
// Name: String
// Type: String
// length: i32
// is_nullable: bool
// is_pkey: bool

struct Column {
    name: String,
    column_type: String,
    length: i32,
    is_nullable: bool,
    is_pkey: bool,
}

//struct FakeSql
// Table: Table
// columns: Vec<column>

struct FakeSql {
    table: Table,
    columns: Vec<Column>,
}

//impl FakeSql
//fn init(table: Table, columns: Vec<column>) -> FakeSql
//fn addColumn(column: column) -> FakeSql
//fn generate() -> String

impl FakeSql {
    fn init(table: Table, columns: Vec<Column>) -> FakeSql {
        FakeSql { table, columns }
    }

    fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    fn init_via_sql(create_table_string: &str) -> FakeSql {
        let parts: Vec<&str> = create_table_string
            .trim_start_matches("create table ")
            .split('(')
            .collect();
        let table_name = parts[0].trim().to_string();

        let columns_part = parts[1].trim_end_matches(')').trim();
        let columns_str: Vec<&str> = columns_part.split(',').collect();

        let mut columns = vec![];
        let table = Table {
            name: table_name,
            ref_table: String::new(),
            ref_column: String::new(),
        };

        for column_str in columns_str {
            let column_parts: Vec<&str> = column_str.trim().split_whitespace().collect();
            let name = column_parts[0];
            let column_type = column_parts[1];
            let is_pkey = column_parts.len() > 2 && column_parts[2] == "primary" && column_parts[3] == "key";

            columns.push(Column {
                name: name.to_string(),
                column_type: column_type.to_string(),
                length: 0, // Length is not provided in the string, so we set it to 0
                is_nullable: !is_pkey, // Assume non-primary key columns are nullable
                is_pkey,
            });
        }

        FakeSql {
            table,
            columns,
        }
    }

    fn generate(&self, sql_type: SqlType) -> String {
        match sql_type {
            SqlType::CreateTable => {
                let mut sql = format!("CREATE TABLE {} (\n", self.table.name);
                for column in &self.columns {
                    sql.push_str(&format!(
                        "    {} {}({}) {} {},\n",
                        column.name,
                        column.column_type,
                        column.length,
                        if column.is_nullable { "NULL" } else { "NOT NULL" },
                        if column.is_pkey { "PRIMARY KEY" } else { "" }
                    ));
                }
                sql.push_str(");");
                sql
            }
            SqlType::AlterTable => {
                let mut sql = format!("ALTER TABLE {} ", self.table.name);
                for column in &self.columns {
                    sql.push_str(&format!(
                        "ADD COLUMN {} {}({}) {} {}, ",
                        column.name,
                        column.column_type,
                        column.length,
                        if column.is_nullable { "NULL" } else { "NOT NULL" },
                        if column.is_pkey { "PRIMARY KEY" } else { "" }
                    ));
                }
                sql.trim_end_matches(", ").to_string() + ";"
            }
            SqlType::DropTable => format!("DROP TABLE {};", self.table.name),
            SqlType::Insert => {
                let column_names: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
                let values: Vec<String> = self.columns.iter().map(|_| "?".to_string()).collect();
                format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    self.table.name,
                    column_names.join(", "),
                    values.join(", ")
                )
            }
            SqlType::Select => {
                let column_names: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
                format!(
                    "SELECT {} FROM {};",
                    column_names.join(", "),
                    self.table.name
                )
            }
            SqlType::Update => {
                let column_names: Vec<String> = self.columns.iter().map(|c| format!("{} = ?", c.name)).collect();
                format!(
                    "UPDATE {} SET {} WHERE <condition>;",
                    self.table.name,
                    column_names.join(", ")
                )
            }
            SqlType::Delete => format!("DELETE FROM {} WHERE <condition>;", self.table.name),
        }
    }
}


fn main() {
    // get env for generate number of records for generate
    let num_records = std::env::var("NUM_RECORDS").unwrap_or("10".to_string()).parse::<i32>().unwrap();

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("output.sql")
        .expect("Unable to open file");

    let order: FakeSql = FakeSql::init_via_sql("create table orders(order_id number(10) primary key, order_date date, customer_id number(10))");
    let customers: FakeSql = FakeSql::init_via_sql("create table customers(customer_id number(10) primary key, customer_name varchar(255), customer_email varchar(255))");
    let products: FakeSql = FakeSql::init_via_sql("create table products(product_id number(10) primary key, product_name varchar(255), product_price number(10, 2))");

    let tables = vec![order, customers, products];

    let sql_types = vec![
        SqlType::CreateTable,
        SqlType::AlterTable,
        SqlType::DropTable,
        SqlType::Insert,
        SqlType::Select,
        SqlType::Update,
        SqlType::Delete,
    ];

    //write to file
    for _ in 0..num_records {
        let mut rng = thread_rng();
        let random_sql_type = sql_types.choose(&mut rng).unwrap();
        let random_table = tables.choose(&mut rng).unwrap();

        let sql = random_table.generate(*random_sql_type);
        writeln!(file, "{}", sql).expect("Unable to write to file");
    }
}
