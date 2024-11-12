### sql-fake

This program generates random SQL statements and writes them to a file named output.sql. The number of SQL statements to generate is determined by the NUM_RECORDS environment variable, defaulting to 30 if the variable is not set.

The program defines three tables: orders, customers, and products, and generates random SQL statements for these tables, including CREATE TABLE, ALTER TABLE, DROP TABLE, INSERT, SELECT, UPDATE, and DELETE operations.

## Usage
Set the NUM_RECORDS environment variable to specify the number of SQL statements to generate. If not set, the program defaults to generating 30 SQL statements.

```bash
export NUM_RECORDS=50
cargo run
```


## api
The generated SQL statements are appended to the output.sql file in the current directory. Initializes a new Table with the given name and columns.

Example
```rust
let columns = vec![
   Column {
       name: "id".to_string(),
       column_type: "number".to_string(),
       length: Some(10),
       decimal_places: None,
       is_nullable: false,
       is_pkey: true,
       ref_table: None,
       ref_column: None,
   },
   Column {
       name: "name".to_string(),
       column_type: "varchar".to_string(),
       length: Some(255),
       decimal_places: None,
       is_nullable: true,
       is_pkey: false,
       ref_table: None,
       ref_column: None,
   },
];
```

## test
```rust
let table = Table::init("test_table".to_string(), columns);
assert_eq!(table.name, "test_table");
assert_eq!(table.columns.len(), 2);
```

## To Run simple test

  // Initialize tables
  let order: Table = Table::init_via_sql("create table orders(order_id number(10) primary key, order_date date, customer_id number(10))");
  let sql = order.generate(SqlType::CreateTable);


## main
```rust
use for main.rs

mod models;
 
use models::{Table, SqlType};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::OpenOptions;
use std::io::Write;
 
fn main() {
    // Get the number of records to generate from the environment variable `NUM_RECORDS`
    let num_records = std::env::var("NUM_RECORDS").unwrap_or("30".to_string()).parse::<i32>().unwrap();
 
    // Open the output file in append mode, creating it if it doesn't exist
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("output.sql")
        .expect("Unable to open file");
 
    // Initialize tables
    let order: Table = Table::init_via_sql("create table orders(order_id number(10) primary key, order_date date, customer_id number(10))");
    let customers: Table = Table::init_via_sql("create table customers(customer_id number(10) primary key, customer_name varchar(255), customer_email varchar(255))");
    let products: Table = Table::init_via_sql("create table products(product_id number(10) primary key, product_name varchar(255), product_price number(10, 2))");
 
    let tables = vec![order, customers, products];
 
    // Define SQL types
    let sql_types = vec![
        SqlType::CreateTable,
        SqlType::AlterTable,
        SqlType::DropTable,
        SqlType::Insert,
        SqlType::Select,
        SqlType::Update,
        SqlType::Delete,
    ];
 
    // Generate and write SQL statements to the file
    for _ in 0..num_records {
        let mut rng = thread_rng();
        let random_sql_type = sql_types.choose(&mut rng).unwrap();
        let random_table = tables.choose(&mut rng).unwrap();
 
        let sql = random_table.generate(*random_sql_type);
        writeln!(file, "{}", sql).expect("Unable to write to file");
    }
}
```