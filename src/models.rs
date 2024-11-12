use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use chrono::{NaiveDate, Duration};
use regex::Regex;

/// Enum representing different types of SQL operations.
#[derive(Copy, Clone)]
pub enum SqlType {
    CreateTable,
    AlterTable,
    DropTable,
    Insert,
    Select,
    Update,
    Delete,
}

/// Struct representing a database table.
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub comment: Option<String>,
}

/// Struct representing a column in a database table.
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub length: Option<i32>,
    pub decimal_places: Option<i32>,
    pub is_nullable: bool,
    pub is_pkey: bool,
    pub ref_table: Option<String>,
    pub ref_column: Option<String>,
}

impl Table {
    /// Initializes a new `Table` with the given name and columns.
    ///
    /// # Arguments
    ///
    /// * `name` - A string slice that holds the name of the table.
    /// * `columns` - A vector of `Column` structs representing the columns of the table.
    ///
    /// # Returns
    ///
    /// A `Table` struct.
    ///
    /// # Example
    ///
    /// ```
    /// let columns = vec![
    ///     Column {
    ///         name: "id".to_string(),
    ///         column_type: "number".to_string(),
    ///         length: Some(10),
    ///         decimal_places: None,
    ///         is_nullable: false,
    ///         is_pkey: true,
    ///         ref_table: None,
    ///         ref_column: None,
    ///     },
    ///     Column {
    ///         name: "name".to_string(),
    ///         column_type: "varchar".to_string(),
    ///         length: Some(255),
    ///         decimal_places: None,
    ///         is_nullable: true,
    ///         is_pkey: false,
    ///         ref_table: None,
    ///         ref_column: None,
    ///     },
    /// ];
    /// let table = Table::init("test_table".to_string(), columns);
    /// assert_eq!(table.name, "test_table");
    /// assert_eq!(table.columns.len(), 2);
    /// ```
    pub fn init(name: String, columns: Vec<Column>) -> Table {
        Table {
            name,
            columns,
            comment: None,
        }
    }

    /// Initializes a new `Table` from a SQL create table string.
    ///
    /// # Arguments
    ///
    /// * `create_table_string` - A string slice that holds the SQL create table statement.
    ///
    /// # Returns
    ///
    /// A `Table` struct.
    ///
    /// # Example
    ///
    /// ```
    /// let sql = "create table test_table (id number(10) primary key, name varchar(255))";
    /// let table = Table::init_via_sql(sql);
    /// assert_eq!(table.name, "test_table");
    /// assert_eq!(table.columns.len(), 2);
    /// assert_eq!(table.columns[0].name, "id");
    /// assert_eq!(table.columns[1].name, "name");
    /// ```
    pub fn init_via_sql(create_table_string: &str) -> Table {
        let create_table_string = create_table_string.to_lowercase().trim().to_string();
        let comment = None;
        let parts: Vec<&str> = create_table_string
            .trim_start_matches("create table ")
            .splitn(2, '(')
            .collect();
        let table_name = parts[0].trim().to_string();

        let trimmed_columns = parts[1].rsplitn(2, ')').collect::<Vec<&str>>()[1].trim();
        let re = Regex::new(r"(\d+)\s*,\s*(\d+)").unwrap();
        let cleaned_columns = re.replace_all(trimmed_columns, "$1.$2").to_string();
        let split_column_strings: Vec<&str> = cleaned_columns.split(',').collect();

        let mut columns = vec![];

        for column_str in split_column_strings {
            let column_parts: Vec<&str> = column_str.trim().split_whitespace().collect();
            let name = column_parts[0];
            let column_type_str = column_parts[1];
            let re = Regex::new(r"([a-zA-Z]+)|(\d+)").unwrap();
            let col_parts = re.find_iter(column_type_str).map(|m| m.as_str()).collect::<Vec<&str>>();

            let mut column_type = "";
            let mut length = None;
            let mut decimal_places = None;

            for (i, part) in col_parts.iter().enumerate() {
                match i {
                    0 => column_type = part,
                    1 => length = part.parse().ok(),
                    2 => decimal_places = part.parse().ok(),
                    _ => (),
                }
            }

            let is_pkey = column_parts.contains(&"primary") && column_parts.contains(&"key");
            let (ref_table, ref_column) = Table::parse_references(&column_parts);

            columns.push(Column {
                name: name.to_string(),
                column_type: column_type.to_string(),
                length,
                decimal_places,
                is_nullable: !is_pkey, // Assume non-primary key columns are nullable
                is_pkey,
                ref_table,
                ref_column,
            });
        }

        Table {
            name: table_name,
            columns,
            comment,
        }
    }

    /// Parses references from column parts.
    ///
    /// # Arguments
    ///
    /// * `column_parts` - A slice of string slices representing parts of a column definition.
    ///
    /// # Returns
    ///
    /// A tuple containing the referenced table and column, if any.
    pub fn parse_references(column_parts: &[&str]) -> (Option<String>, Option<String>) {
        if let Some(pos) = column_parts.iter().position(|&s| s == "references") {
            let ref_table = column_parts.get(pos + 1).map(|s| s.to_string());
            let ref_column = column_parts.get(pos + 2).map(|s| s.trim_matches(|c| c == '(' || c == ')').to_string());
            (ref_table, ref_column)
        } else {
            (None, None)
        }
    }

    /// Generates a SQL WHERE clause based on the table's columns.
    ///
    /// # Returns
    ///
    /// A string representing the SQL WHERE clause.
    pub fn generate_where_clause(&self) -> String {
        let mut rng = thread_rng();
        let mut conditions = vec![];

        for column in &self.columns {
            let condition = match column.column_type.as_str() {
                "int" | "number" => {
                    let operator = ["=", ">", "<", ">=", "<="].choose(&mut rng).unwrap();
                    format!("{} {} {}", column.name, operator, rng.gen_range(1..100))
                }
                "varchar" | "text" => {
                    let values: Vec<String> = (0..rng.gen_range(2..11))
                        .map(|_| format!("'{}'", ["Alice", "Bob", "Charlie", "David"].choose(&mut rng).unwrap()))
                        .collect();
                    format!("{} IN ({})", column.name, values.join(", "))
                }
                "date" | "datetime" => {
                    let start_date = NaiveDate::from_ymd(2021, 1, 1) + Duration::days(rng.gen_range(0..3));
                    let end_date = chrono::Local::today().naive_local();
                    format!("{} BETWEEN to_date('{}','YYYY-MM-DD') AND to_date('{}','YYYY-MM-DD')", column.name, start_date, end_date)
                }
                _ => continue,
            };
            conditions.push(condition);
        }

        conditions.join(" AND ")
    }

    /// Generates a SQL statement based on the table and SQL type.
    ///
    /// # Arguments
    ///
    /// * `sql_type` - The type of SQL statement to generate.
    ///
    /// # Returns
    ///
    /// A string representing the SQL statement.
    ///
    /// # Example
    ///
    /// ```
    /// let columns = vec![
    ///     Column {
    ///         name: "id".to_string(),
    ///         column_type: "number".to_string(),
    ///         length: Some(10),
    ///         decimal_places: None,
    ///         is_nullable: false,
    ///         is_pkey: true,
    ///         ref_table: None,
    ///         ref_column: None,
    ///     },
    ///     Column {
    ///         name: "name".to_string(),
    ///         column_type: "varchar".to_string(),
    ///         length: Some(255),
    ///         decimal_places: None,
    ///         is_nullable: true,
    ///         is_pkey: false,
    ///         ref_table: None,
    ///         ref_column: None,
    ///     },
    /// ];
    /// let table = Table::init("test_table".to_string(), columns);
    /// let sql = table.generate(SqlType::CreateTable);
    /// assert_eq!(sql, "CREATE TABLE test_table (id number(10) NOT NULL PRIMARY KEY, name varchar(255));");
    /// ```
    pub fn generate(&self, sql_type: SqlType) -> String {
        match sql_type {
            SqlType::CreateTable => {
                let mut sql = format!("CREATE TABLE {} (", self.name);
                for column in &self.columns {
                    sql.push_str(&format!(
                        "{} {}{}{}{}{}",
                        column.name,
                        column.column_type,
                        if let Some(length) = column.length {
                            if let Some(decimal_places) = column.decimal_places {
                                format!("({},{})", length, decimal_places)
                            } else {
                                format!("({})", length)
                            }
                        } else {
                            "".to_string()
                        },
                        if column.is_nullable { "" } else { " NOT NULL" },
                        if column.is_pkey { " PRIMARY KEY" } else { "" },
                        if self.columns.last().unwrap().name != column.name { ", " } else { "" }
                        
                    ));
                }
                sql.push_str(");");
                sql
            }
            SqlType::AlterTable => {
                let mut sql = format!("ALTER TABLE {} ", self.name);
                for column in &self.columns {
                    sql.push_str(&format!(
                        "ADD COLUMN {} {}{}{}{}{}",
                        column.name,
                        column.column_type,
                        if let Some(length) = column.length {
                            if let Some(decimal_places) = column.decimal_places {
                                format!("({},{})", length, decimal_places)
                            } else {
                                format!("({})", length)
                            }
                        } else {
                            "".to_string()
                        },
                        if column.is_nullable { "" } else { " NOT NULL" },
                        if column.is_pkey { " PRIMARY KEY" } else { "" },
                        if self.columns.last().unwrap().name != column.name { ", " } else { "" }
                    ));
                }
                sql.trim_end_matches(", ").to_string() + ";"
            }
            SqlType::DropTable => format!("DROP TABLE {};", self.name),
            SqlType::Insert => {
                let mut rng = thread_rng();
                let column_names: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
                let values: Vec<String> = self.columns.iter().map(|c| {
                    match c.column_type.as_str() {
                        "varchar" | "text" => format!("'{}'", ["Alice", "Bob", "Charlie", "David"].choose(&mut rng).unwrap()),
                        "date" | "datetime" => {
                            let today = chrono::Local::today().naive_local();
                            format!("to_date('{}','YYYY-MM-DD')", today)
                        },
                        "number" if c.decimal_places.is_some() => {
                            let factor = 10f64.powi(c.decimal_places.unwrap());
                            let value = rng.gen_range(1..100) as f64 / factor;
                            format!("{:.1$}", value, c.decimal_places.unwrap() as usize)
                        }
                        _ => rng.gen_range(1..100).to_string(),
                    }
                }).collect();
                format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    self.name,
                    column_names.join(", "),
                    values.join(", ")
                )
            }
            SqlType::Select => {
                let column_names: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
                format!(
                    "SELECT {} FROM {} WHERE {};",
                    column_names.join(", "),
                    self.name,
                    self.generate_where_clause()
                )
            }
            SqlType::Update => {
                let mut rng = thread_rng();
                let column_values: Vec<String> = self.columns.iter().map(|c| {
                    match c.column_type.as_str() {
                        "varchar" | "text" => format!("{} = '{}'", c.name, ["Alice", "Bob", "Charlie", "David"].choose(&mut rng).unwrap()),
                        "date" | "datetime" => {
                            let today = chrono::Local::today().naive_local();
                            format!("{} = to_date('{}','YYYY-MM-DD')", c.name, today)
                        },
                        "number" if c.decimal_places.is_some() => {
                            let factor = 10f64.powi(c.decimal_places.unwrap());
                            let value = rng.gen_range(1..100) as f64 / factor;
                            format!("{} = {:.precision$}", c.name, value, precision = c.decimal_places.unwrap() as usize)
                        }
                        _ => format!("{} = {}", c.name, rng.gen_range(1..100)),
                    }
                }).collect();
                format!(
                    "UPDATE {} SET {} WHERE {};",
                    self.name,
                    column_values.join(", "),
                    self.generate_where_clause()
                )
            }
            SqlType::Delete => format!("DELETE FROM {} WHERE {};", self.name, self.generate_where_clause()),
        }
    }
    
    /// Sets a comment for the table.
    ///
    /// # Arguments
    ///
    /// * `comment` - An optional string slice that holds the comment for the table.
    pub fn set_comment(&mut self, comment: Option<String>) {
        self.comment = comment;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_init() {
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
        let table = Table::init("test_table".to_string(), columns);
        assert_eq!(table.name, "test_table");
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn test_table_init_via_sql() {
        let sql = "create table test_table (id number(10) primary key, name varchar(255))";
        let table = Table::init_via_sql(sql);
        assert_eq!(table.name, "test_table");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[1].name, "name");
    }

    #[test]
    fn test_generate_create_table() {
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
        let table = Table::init("test_table".to_string(), columns);
        let sql = table.generate(SqlType::CreateTable);
        assert_eq!(sql, "CREATE TABLE test_table (id number(10) NOT NULL PRIMARY KEY, name varchar(255));");
    }
}