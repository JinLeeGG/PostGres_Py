import psycopg2
import sys
import traceback
import re  # For collapsing whitespace, like the Java 'replaceAll(" +", " ")'

class QueryExecutor:
    # --- Class variables translated from Java static finals ---
    PG_USER = "postgres"
    PG_PASSWORD = ""
    PG_HOST = "localhost"
    PG_PORT_NUMBER = "5432"
    
    # Java: private static String SUPABASE_CONN = null;
    _SUPABASE_CONN = None

    def __init__(self, dbname, schema_name="wine"):
        """
        Python's constructor, equivalent to the two Java constructors.
        It accepts a required 'dbname' and an optional 'schema_name'.
        """
        # Java: private String schema_name = "wine";
        self._schema_name = schema_name
        self._dbname = dbname
        
        # Java: Connection connection = null;
        self.connection = None
        
        # Call the initialize method, just like the Java constructor
        self._initialize()

    def _initialize(self):
        """
        Equivalent to the private void initialize() method in Java.
        Establishes the connection and sets the schema.
        """
        try:
            if self._SUPABASE_CONN is None:
                # This is the equivalent of the Java 'Properties' and 'DriverManager.getConnection'
                self.connection = psycopg2.connect(
                    dbname=self._dbname,
                    user=QueryExecutor.PG_USER,
                    password=QueryExecutor.PG_PASSWORD,
                    host=QueryExecutor.PG_HOST,
                    port=QueryExecutor.PG_PORT_NUMBER,
                    
                    # *** FIX FOR ENCODING ERROR ***
                    # Set this to the value you found in psql
                    client_encoding='UTF8' 
                )
            else:
                # Logic to handle the SUPABASE_CONN if it were set
                self.connection = psycopg2.connect(
                    self._SUPABASE_CONN,
                    client_encoding='UTF8'
                )
                
            # *** CRITICAL ***
            # This matches the Java code's behavior where 'execute'
            # works without needing a 'commit()'.
            self.connection.autocommit = True

        except psycopg2.Error as e:
            self._handle_sql_exception(e)

        print("Successfully connected to Postgres.")
        
        # Java: this.execute("set search_path to " + this.schema_name);
        try:
            with self.connection.cursor() as cursor:
                # Safely set the search path, adding 'public' is good practice
                cursor.execute(f"SET search_path TO {psycopg2.extensions.quote_ident(self._schema_name, self.connection)}, public;")
        except psycopg2.Error as e:
             self._handle_sql_exception(e)
             
        # Java: System.out.format("Using schema '%s'.\n\n", this.schema_name);
        print(f"Using schema '{self._schema_name}'.\n")

    def get_connection(self):
        """ Equivalent to public Connection getConnection() """
        return self.connection

    def _handle_sql_exception(self, e):
        """ Equivalent to public void handleSQLException(SQLException var1) """
        # Java: var1.printStackTrace();
        traceback.print_exc()
        # Java: System.exit(0);
        print("\nExiting due to SQL error.")
        sys.exit(0)

    def _print_result_set(self, cursor):
        """ 
        Equivalent to private void printResultSet(ResultSet var1)
        Takes a psycopg2 cursor object *after* it has executed.
        """
        print("Result set:")
        try:
            # Java: ResultSetMetaData var2 = var1.getMetaData();
            # Headers are in cursor.description
            if cursor.description is None:
                print("(No results to display)")
                return

            # Java: int var3 = var2.getColumnCount();
            headers = [col.name for col in cursor.description]
            
            # Java: Printing headers loop
            print(" | ".join(headers))
            
            # Java: while(var1.next())
            results = cursor.fetchall()
            row_count = 0
            for row in results:
                row_count += 1
                # Java: Building the string row
                print(" | ".join(map(str, row)))

            # Java: System.out.println("Number of rows: " + var4);
            print(f"Number of rows: {row_count}")

        except psycopg2.Error as e:
            self._handle_sql_exception(e)

    def _print_execution_message(self, query):
        """ Equivalent to private void printExecutionMessage(String var1) """
        if len(query) > 55:
            msg = query[:55] + "..."
        else:
            msg = query
            
        # Java: var2 = var2.replace('\n', ' ').trim().replaceAll(" +", " ");
        # This is the Python way to "collapse" all whitespace
        msg = re.sub(r'\s+', ' ', msg.replace('\n', ' ')).strip()
        
        # Java: System.out.format("Executed the query \"%s\"\n", var2);
        print(f'Executed the query "{msg}"')

    def execute(self, query):
        """ 
        Equivalent to public void execute(String var1)
        Used for queries that don't return results (e.g., SET, CREATE, INSERT)
        """
        try:
            # The 'with' block handles 'try-with-resources' and closing
            with self.connection.cursor() as cursor:
                # Java: var2.execute(var1);
                cursor.execute(query)
        except psycopg2.Error as e:
            self._handle_sql_exception(e)
        
        self._print_execution_message(query)

    def execute_query(self, query, print_results=True):
        """
        Equivalent to the overloaded methods:
        public void executeQuery(String var1)
        public void executeQuery(String var1, boolean var2)
        """
        try:
            # 'with' block handles closing the cursor
            with self.connection.cursor() as cursor:
                # Java: ResultSet var4 = var3.executeQuery(var1);
                cursor.execute(query)
                
                # Java: this.printExecutionMessage(var1);
                self._print_execution_message(query)
                
                # Java: if (var2) { this.printResultSet(var4); }
                if print_results:
                    self._print_result_set(cursor)
        except psycopg2.Error as e:
            self._handle_sql_exception(e)

    def close(self):
        """ A helper method to properly close the connection """
        if self.connection:
            self.connection.close()
            print("Database connection closed.")


# --- Main execution block ---
# Equivalent to public static void main(String[] var0)
if __name__ == "__main__":
    
    # *** 1. SET YOUR DATABASE NAME HERE ***
    # We use 'postgres' as confirmed from your psql output
    DB_NAME = "postgres"
    
    qe = None # Define qe outside try for the 'finally' block
    try:
        # Java: QueryExecutor var1 = new QueryExecutor("wine");
        qe = QueryExecutor(dbname=DB_NAME, schema_name="wine")
        
        # --- ALL YOUR QUERIES FROM THE FIRST SCRIPT ---
        
        print("////////////////////////////////")
        print("//////////// Question 7.9E  ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT s.SUPNAME, s.SUPNR
            FROM supplier s
            WHERE NOT EXISTS (
                SELECT 1 FROM purchase_order p WHERE s.SUPNR = p.SUPNR
            )
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.11E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT SUM(available_quantity) AS TOTAL_QUANTITY
            FROM product
            WHERE prodtype = 'sparkling'
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.12E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT s.supnr, s.supname, COUNT(po.ponr) AS total_num_of_outstanding_orders
            FROM supplier s
            LEFT JOIN purchase_order po ON s.supnr = po.supnr
            GROUP BY s.supnr, s.supname
            ORDER BY s.supnr
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.13E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT s.supnr, COUNT(s.prodnr) AS num_of_product
            FROM supplies s
            GROUP BY s.supnr
            HAVING COUNT(s.prodnr) > 5
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.14E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT s.supnr, s.supname, AVG(sp.deliv_period) AS average_delivery_time
            FROM supplier s
            JOIN supplies sp ON s.supnr = sp.supnr
            GROUP BY s.supnr, s.supname
            ORDER BY s.supnr
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.15E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT DISTINCT ponr
            FROM po_line
            WHERE prodnr IN (
                SELECT p.prodnr
                FROM product p
                WHERE prodtype = 'sparkling' OR prodtype = 'red'
            )
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.16E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT p1.prodnr
            FROM product p1
            WHERE 3 >= (
                SELECT COUNT(p2.prodnr)
                FROM product p2
                WHERE p2.prodnr <= p1.prodnr
            )
            ORDER BY p1.prodnr ASC
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.17E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT prodname, available_quantity
            FROM product
            WHERE available_quantity >= ALL (
                SELECT available_quantity
                FROM product
                WHERE available_quantity IS NOT NULL
            )
        """)

        print("////////////////////////////////")
        print("//////////// Question 7.18E ////")
        print("////////////////////////////////")
        qe.execute_query("""
            SELECT s1.supname, s1.supnr
            FROM supplier s1
            WHERE NOT EXISTS(
                SELECT 1
                FROM supplier s2
                WHERE s2.supnr < s1.supnr
            )
        """)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        # Always close the connection
        if qe:
            qe.close()