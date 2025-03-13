#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <libpq-fe.h>

using namespace std;

bool g_printNotices = false;

void myNoticeProcessor(void* arg, const char* message) {
    bool* printNotices = static_cast<bool*>(arg);
    if (*printNotices) {
        cerr << message;
    }
}

struct Book {
    int id;
    string title;
    string author;
    string publisher;
    int year;
};

void checkResult(PGresult* res, PGconn* conn, const string& errMsg) {
    if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
        string error = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(errMsg + ": " + error);
    }
}

class DBManager {
public:
    DBManager(const string& dbName, const string& user, const string& password)
        : dbName_(dbName), user_(user), password_(password) {
        connect();
    }

    ~DBManager() {
        if (conn_) {
            PQfinish(conn_);
        }
    }

    void initProcedures() {
        string proceduresSQL = R"sql(
            CREATE EXTENSION IF NOT EXISTS dblink;

            CREATE OR REPLACE PROCEDURE sp_create_database(p_dbname VARCHAR)
            LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM dblink_exec(
                    'host=localhost dbname=postgres user=' || current_user,
                    'CREATE DATABASE ' || quote_ident(p_dbname)
                );
                RAISE NOTICE 'Database "%" created.', p_dbname;
            END;
            $$;

            CREATE OR REPLACE PROCEDURE sp_drop_database(p_dbname VARCHAR)
            LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM dblink_exec(
                    'host=localhost dbname=postgres user=' || current_user,
                    'DO $inner$
                     BEGIN
                       PERFORM pg_terminate_backend(pid)
                       FROM pg_stat_activity
                       WHERE datname = ' || quote_literal(p_dbname) || ' AND pid <> pg_backend_pid();
                     END $inner$;'
                );
                PERFORM dblink_exec(
                    'host=localhost dbname=postgres user=' || current_user,
                    'DROP DATABASE IF EXISTS ' || quote_ident(p_dbname)
                );
                RAISE NOTICE 'Database "%" dropped.', p_dbname;
            END;
            $$;

            CREATE OR REPLACE PROCEDURE sp_create_table(p_tablename VARCHAR)
            LANGUAGE plpgsql AS $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND lower(table_name) = lower(p_tablename)
                ) THEN
                     RAISE NOTICE 'Table "%" already exists.', p_tablename;
                ELSE
                     EXECUTE format('CREATE TABLE %I (
                         id SERIAL PRIMARY KEY,
                         title VARCHAR(255),
                         author VARCHAR(255),
                         publisher VARCHAR(255),
                         year INT
                     )', p_tablename);
                     RAISE NOTICE 'Table "%" created.', p_tablename;
                END IF;
            END;
            $$;

            CREATE OR REPLACE PROCEDURE sp_clear_table(p_tablename VARCHAR)
            LANGUAGE plpgsql AS $$
            BEGIN
                EXECUTE format('TRUNCATE TABLE %I', p_tablename);
                RAISE NOTICE 'Table "%" cleared.', p_tablename;
            END;
            $$;

            CREATE OR REPLACE PROCEDURE sp_add_book(
                p_tablename VARCHAR,
                p_title VARCHAR,
                p_author VARCHAR,
                p_publisher VARCHAR,
                p_year INT
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                EXECUTE format(
                  'INSERT INTO %I (title, author, publisher, year) VALUES (%L, %L, %L, %s)',
                  p_tablename, p_title, p_author, p_publisher, p_year
                );
                RAISE NOTICE 'Book added: %', p_title;
            END;
            $$;

            CREATE OR REPLACE FUNCTION sp_search_book_by_title(p_tablename VARCHAR, p_title VARCHAR)
            RETURNS TABLE(
                id INT,
                title VARCHAR,
                author VARCHAR,
                publisher VARCHAR,
                year INT
            )
            LANGUAGE plpgsql SECURITY DEFINER AS $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND lower(table_name) = lower(p_tablename)
                ) THEN
                    RETURN;
                ELSE
                    RETURN QUERY EXECUTE format(
                        'SELECT id, title, author, publisher, year FROM %I WHERE title ILIKE %L',
                        p_tablename, '%' || p_title || '%'
                    );
                END IF;
            END;
            $$;

            CREATE OR REPLACE PROCEDURE sp_update_book(
                p_tablename VARCHAR,
                p_id INT,
                p_title VARCHAR,
                p_author VARCHAR,
                p_publisher VARCHAR,
                p_year INT
            )
            LANGUAGE plpgsql AS $$
            BEGIN
                EXECUTE format(
                  'UPDATE %I SET title=%L, author=%L, publisher=%L, year=%s WHERE id=%s',
                  p_tablename, p_title, p_author, p_publisher, p_year, p_id
                );
                RAISE NOTICE 'Book updated with id: %', p_id;
            END;
            $$;

            CREATE OR REPLACE PROCEDURE sp_delete_book_by_title(p_tablename VARCHAR, p_title VARCHAR)
            LANGUAGE plpgsql AS $$
            BEGIN
                EXECUTE format(
                  'DELETE FROM %I WHERE title=%L',
                  p_tablename, p_title
                );
                RAISE NOTICE 'Book(s) with title "%" deleted.', p_title;
            END;
            $$;

            -- New procedure to create a DB user with a given access mode.
            CREATE OR REPLACE PROCEDURE sp_create_db_user(p_username VARCHAR, p_password VARCHAR, p_mode VARCHAR)
            LANGUAGE plpgsql AS $$
            BEGIN
                EXECUTE format('CREATE USER %I WITH PASSWORD %L', p_username, p_password);
                IF lower(p_mode) = 'admin' THEN
                    EXECUTE format('ALTER USER %I WITH SUPERUSER', p_username);
                ELSE
                    EXECUTE format('ALTER USER %I WITH NOSUPERUSER', p_username);
                END IF;
                RAISE NOTICE 'User "%" created with mode %.', p_username, p_mode;
            END;
            $$;
        )sql";

        PGresult* res = PQexec(conn_, proceduresSQL.c_str());
        checkResult(res, conn_, "Error initializing stored procedures");
        PQclear(res);
    }

    void createDatabase(const string& newDbName) {
        PGconn* connTemp = PQsetdbLogin("localhost", "5432", nullptr, nullptr, "postgres", user_.c_str(), password_.c_str());
        if (PQstatus(connTemp) != CONNECTION_OK) {
            string error = PQerrorMessage(connTemp);
            PQfinish(connTemp);
            throw runtime_error("Error connecting to postgres: " + error);
        }
        string sql = "CALL sp_create_database($1)";
        const char* paramValues[1] = { newDbName.c_str() };
        PGresult* res = PQexecParams(connTemp, sql.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, connTemp, "Error creating database");
        PQclear(res);
        PQfinish(connTemp);
    }

    void dropDatabase(const string& dbNameToDrop) {
        PGconn* connTemp = PQsetdbLogin("localhost", "5432", nullptr, nullptr, "postgres", user_.c_str(), password_.c_str());
        if (PQstatus(connTemp) != CONNECTION_OK) {
            string error = PQerrorMessage(connTemp);
            PQfinish(connTemp);
            throw runtime_error("Error connecting to postgres: " + error);
        }
        string sql = "CALL sp_drop_database($1)";
        const char* paramValues[1] = { dbNameToDrop.c_str() };
        PGresult* res = PQexecParams(connTemp, sql.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, connTemp, "Error dropping database");
        PQclear(res);
        PQfinish(connTemp);
    }

    void createTable(const string& tableName) {
        string sql = "CALL sp_create_table($1)";
        const char* paramValues[1] = { tableName.c_str() };
        PGresult* res = PQexecParams(conn_, sql.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, conn_, "Error creating table");
        PQclear(res);
    }

    void clearTable(const string& tableName) {
        string sql = "CALL sp_clear_table($1)";
        const char* paramValues[1] = { tableName.c_str() };
        PGresult* res = PQexecParams(conn_, sql.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, conn_, "Error clearing table");
        PQclear(res);
    }

    void addBook(const string& tableName, const string& title,
                 const string& author, const string& publisher, int year) {
        string sql = "CALL sp_add_book($1, $2, $3, $4, $5)";
        string yearStr = to_string(year);
        const char* paramValues[5] = { tableName.c_str(), title.c_str(), author.c_str(), publisher.c_str(), yearStr.c_str() };
        PGresult* res = PQexecParams(conn_, sql.c_str(), 5, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, conn_, "Error adding book");
        PQclear(res);
    }

    vector<Book> searchBookByTitle(const string& tableName, const string& titleFilter) {
        vector<Book> books;
        string sql = "SELECT * FROM sp_search_book_by_title($1, $2)";
        const char* paramValues[2] = { tableName.c_str(), titleFilter.c_str() };
        PGresult* res = PQexecParams(conn_, sql.c_str(), 2, nullptr, paramValues, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res);
            throw runtime_error("Error searching for book");
        }
        int nRows = PQntuples(res);
        for (int i = 0; i < nRows; ++i) {
            Book b;
            b.id = stoi(PQgetvalue(res, i, 0));
            b.title = PQgetvalue(res, i, 1);
            b.author = PQgetvalue(res, i, 2);
            b.publisher = PQgetvalue(res, i, 3);
            b.year = stoi(PQgetvalue(res, i, 4));
            books.push_back(b);
        }
        PQclear(res);
        return books;
    }

    void updateBook(const string& tableName, int id, const string& title,
                    const string& author, const string& publisher, int year) {
        string sql = "CALL sp_update_book($1, $2, $3, $4, $5, $6)";
        string idStr = to_string(id);
        string yearStr = to_string(year);
        const char* paramValues[6] = { tableName.c_str(), idStr.c_str(), title.c_str(), author.c_str(), publisher.c_str(), yearStr.c_str() };
        PGresult* res = PQexecParams(conn_, sql.c_str(), 6, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, conn_, "Error updating book");
        PQclear(res);
    }

    void deleteBookByTitle(const string& tableName, const string& title) {
        string sql = "CALL sp_delete_book_by_title($1, $2)";
        const char* paramValues[2] = { tableName.c_str(), title.c_str() };
        PGresult* res = PQexecParams(conn_, sql.c_str(), 2, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, conn_, "Error deleting book");
        PQclear(res);
    }

    void createDBUser(const string& newUsername, const string& newPassword, const string& mode) {
        string sql = "CALL sp_create_db_user($1, $2, $3)";
        const char* paramValues[3] = { newUsername.c_str(), newPassword.c_str(), mode.c_str() };
        PGresult* res = PQexecParams(conn_, sql.c_str(), 3, nullptr, paramValues, nullptr, nullptr, 0);
        checkResult(res, conn_, "Error creating DB user");
        PQclear(res);
    }

private:
    void connect() {
        ostringstream conninfo;
        conninfo << "host=localhost port=5432 dbname=" << dbName_
                 << " user=" << user_ << " password=" << password_;
        conn_ = PQconnectdb(conninfo.str().c_str());
        if (PQstatus(conn_) != CONNECTION_OK) {
            string error = PQerrorMessage(conn_);
            PQfinish(conn_);
            throw runtime_error("Error connecting to DB: " + error);
        }
        PQsetNoticeProcessor(conn_, myNoticeProcessor, &g_printNotices);
        PGresult* res = PQexec(conn_, "SET client_min_messages TO warning;");
        PQclear(res);
    }

    string dbName_;
    string user_;
    string password_;
    PGconn* conn_ = nullptr;
};

void printBooks(const vector<Book>& books) {
    if (books.empty()) {
        cout << "No books found." << endl;
        return;
    }
    for (const auto& b : books) {
        cout << b.id << ": " << b.title << " by " << b.author
             << " (" << b.publisher << ", " << b.year << ")" << endl;
    }
}

int main() {
    try {
        string dbName, username, password, tableName;
        cout << "Enter database name: ";
        getline(cin, dbName);
        cout << "Enter username: ";
        getline(cin, username);
        cout << "Enter password: ";
        getline(cin, password);

        bool isAdmin = (username == "admin");

        DBManager dbManager(dbName, username, password);
        dbManager.initProcedures();
        cout << "Connection successful." << endl;

        cout << "Enter table name for operations: ";
        getline(cin, tableName);

        bool exitFlag = false;
        while (!exitFlag) {
            g_printNotices = false;
            cout << "\n" << flush;
            cout << "Available operations:" << endl;
            if (isAdmin) {
                cout << "1. Create database" << endl;
                cout << "2. Drop database" << endl;
                cout << "3. Create table" << endl;
                cout << "4. Clear table" << endl;
                cout << "5. Add book" << endl;
                cout << "6. Update book" << endl;
                cout << "7. Delete book by Title" << endl;
            }
            cout << "8. Search book by Title" << endl;
            cout << "9. View all records" << endl;
            if (isAdmin) {
                cout << "10. Create new DB user" << endl;
                cout << "11. Exit" << endl;
            } else {
                cout << "10. Exit" << endl;
            }
            cout << "Choose an operation: ";
            int choice;
            cin >> choice;
            cin.ignore();

            try {
                g_printNotices = true;
                if (choice == 1 && isAdmin) {
                    string newDb;
                    cout << "Enter the name of the database to create: ";
                    getline(cin, newDb);
                    dbManager.createDatabase(newDb);
                    cout << "Database created." << endl;
                } else if (choice == 2 && isAdmin) {
                    string dropDb;
                    cout << "Enter the name of the database to drop: ";
                    getline(cin, dropDb);
                    dbManager.dropDatabase(dropDb);
                    cout << "Database dropped." << endl;
                } else if (choice == 3 && isAdmin) {
                    dbManager.createTable(tableName);
                    cout << "Table created." << endl;
                } else if (choice == 4 && isAdmin) {
                    dbManager.clearTable(tableName);
                    cout << "Table cleared." << endl;
                } else if (choice == 5 && isAdmin) {
                    string title, author, publisher;
                    int year;
                    cout << "Enter Title: ";
                    getline(cin, title);
                    cout << "Enter Author: ";
                    getline(cin, author);
                    cout << "Enter Publisher: ";
                    getline(cin, publisher);
                    cout << "Enter Year: ";
                    cin >> year;
                    cin.ignore();
                    dbManager.addBook(tableName, title, author, publisher, year);
                    cout << "Book added." << endl;
                } else if (choice == 6 && isAdmin) {
                    int id, year;
                    string title, author, publisher;
                    cout << "Enter the ID of the book to update: ";
                    cin >> id;
                    cin.ignore();
                    cout << "Enter new Title: ";
                    getline(cin, title);
                    cout << "Enter new Author: ";
                    getline(cin, author);
                    cout << "Enter new Publisher: ";
                    getline(cin, publisher);
                    cout << "Enter new Year: ";
                    cin >> year;
                    cin.ignore();
                    dbManager.updateBook(tableName, id, title, author, publisher, year);
                    cout << "Book updated." << endl;
                } else if (choice == 7 && isAdmin) {
                    string title;
                    cout << "Enter the Title of the book to delete: ";
                    getline(cin, title);
                    dbManager.deleteBookByTitle(tableName, title);
                    cout << "Book deleted." << endl;
                } else if (choice == 8) {
                    string title;
                    cout << "Enter part of the Title to search: ";
                    getline(cin, title);
                    vector<Book> books = dbManager.searchBookByTitle(tableName, title);
                    printBooks(books);
                } else if (choice == 9) {
                    vector<Book> books = dbManager.searchBookByTitle(tableName, "");
                    printBooks(books);
                } else if (choice == 10 && isAdmin) {
                    string newUsername, newUserPassword, newUserMode;
                    cout << "Enter new DB username: ";
                    getline(cin, newUsername);
                    cout << "Enter new DB user password: ";
                    getline(cin, newUserPassword);
                    cout << "Enter access mode for new user (admin/guest): ";
                    getline(cin, newUserMode);
                    dbManager.createDBUser(newUsername, newUserPassword, newUserMode);
                    cout << "New DB user created." << endl;
                } else if ((choice == 10 && !isAdmin) || (choice == 11 && isAdmin)) {
                    exitFlag = true;
                } else {
                    cout << "Invalid choice or operation not available for the current role." << endl;
                }
                g_printNotices = false;
            } catch (const exception& ex) {
                cerr << "Error: " << ex.what() << endl;
                g_printNotices = false;
            }
        }
    } catch (const exception& ex) {
        cerr << "Critical error: " << ex.what() << endl;
    }

    return 0;
}
