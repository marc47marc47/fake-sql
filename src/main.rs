mod models;

use models::{Table, SqlType};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::OpenOptions;
use std::io::Write;

fn main() {
    // get env for generate number of records for generate
    let num_records = std::env::var("NUM_RECORDS").unwrap_or("30".to_string()).parse::<i32>().unwrap();

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("output.sql")
        .expect("Unable to open file");

    let order: Table = Table::init_via_sql("create table orders(order_id number(10) primary key, order_date date, customer_id number(10))");
    let customers: Table = Table::init_via_sql("create table customers(customer_id number(10) primary key, customer_name varchar(255), customer_email varchar(255))");
    let products: Table = Table::init_via_sql("create table products(product_id number(10) primary key, product_name varchar(255), product_price number(10, 2))");

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