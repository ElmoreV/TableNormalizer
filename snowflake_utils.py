
import snowflake.connector

def connect_to_snowflake(user, authenticator, account, role):
    return snowflake.connector.connect(
        user=user,
    authenticator = authenticator,
    account = account,
    role = role
    )

def get_cursor(conn):
    return conn.cursor()

def get_columns(cursor, database, schema, table):
    cursor.execute(f"USE {database}.{schema}")
    get_column_query = f"""
    SELECT 
        column_name 
    FROM 
        information_schema.columns 
    WHERE 
        table_catalog = '{database}'
        AND table_schema = '{schema}' 
        AND table_name = '{table}'
    """
    # Remove all whitespace that I added above for readabililty
    " ".join(get_column_query.split())    
    cursor.execute(get_column_query)
    return [row[0] for row in cursor.fetchall()]

def get_distinct_values(cursor, full_table_name, filters=None):
    distinct_value_query = f"""
    SELECT DISTINCT 
    COUNT(*)
    FROM {full_table_name}
    """
    if filters:
        distinct_value_query += f"""
        WHERE {filters}
        """
    cursor.execute(distinct_value_query)
    distinct_values = cursor.fetchone()[0]
    return distinct_values

def get_unique_values(cursor, full_table_name, column_name, filters=None, count_null_as_distinct_value=True):
    if count_null_as_distinct_value:
        unique_value_query = f"""
        SELECT 
            COUNT( DISTINCT COALESCE({column_name}::VARCHAR, 'NULL_PLACEHOLDER') ) as unique_values
        FROM
            {full_table_name}
        """
    else:
        unique_value_query = f"""
        SELECT 
            COUNT( DISTINCT {column_name} ) as unique_values
        FROM
            {full_table_name}
        """
    if filters:
        unique_value_query += f"""
        WHERE {filters}
        """
    cursor.execute(unique_value_query)
    unique_values = cursor.fetchone()[0]
    return unique_values

def get_pairwise_unique_values(cursor, full_table_name, column_name_1, column_name_2, filters=None, count_null_as_distinct_value=True):
    if count_null_as_distinct_value:
        unique_value_query = f"""
        SELECT 
            COUNT( DISTINCT COALESCE({column_name_2}::VARCHAR, 'NULL_PLACEHOLDER') ) as unique_values
        FROM
            {full_table_name}
        """
    else:
        unique_value_query = f"""
        SELECT 
            COUNT( DISTINCT {column_name_2} ) as unique_values
        FROM
            {full_table_name}
        """
    if filters:
        unique_value_query += f"""
        WHERE {filters}
        """
    unique_value_query += f"""
    GROUP BY {column_name_1}
    ORDER BY unique_values DESC
    LIMIT 1
    """
    cursor.execute(unique_value_query)
    unique_values = cursor.fetchone()[0]
    return unique_values

def get_general_unique_values(cursor, full_table_name, lhs_columns, rhs_columns, filters=None, count_null_as_distinct_value=True):
    if rhs_columns is None or len(rhs_columns) == 0:
        return None
    if lhs_columns == None:
        lhs_columns = []
    # Set up all output columns
    select_clause = ""
    groupers = ""
    for rhs_column in rhs_columns:
        if count_null_as_distinct_value:
            select_clause += f"COUNT(DISTINCT COALESCE({rhs_column}::VARCHAR, 'NULL_PLACEHOLDER')) as {rhs_column}, \n"
        else:
            select_clause += f"COUNT(DISTINCT {rhs_column}) as {rhs_column}, \n"
            
    # Add filter if provided
    filter_clause = ""
    if filters:
        filter_clause += f"""
            WHERE {filters}
        """
    
    # Group by all columns (not there is lhs is empty)
    group_clause = ""
    for ii, lhs_column in enumerate(lhs_columns):
        if count_null_as_distinct_value:
            groupers += f"COALESCE({lhs_column}::VARCHAR, 'NULL_PLACEHOLDER')"
        else:
            groupers += f"{lhs_column}"
        if ii < len(lhs_columns) - 1:
            groupers += ",\n"
        else:
            groupers += "\n"
    if len(lhs_columns) > 0:
        group_clause = f"""
            GROUP BY
                {groupers}
        """

    # Select clause to maximize all counts of unique RHS colimssn
    count_maximizers = ""
    for column in rhs_columns:
        count_maximizers += f"MAX({column}) as {column}, \n"
    
    # Build final query
    unique_value_query = f"""
    WITH grouped_counts AS (
        SELECT
        {select_clause}
        FROM
            {full_table_name}
        {filter_clause}
        {group_clause}
    )
    SELECT
        {count_maximizers}
    FROM
        grouped_counts
    """
    # print(f"DEBUG: Running unique value query -> {unique_value_query}")
    # Retrieve and format results
    cursor.execute(unique_value_query)
    unique_values = cursor.fetchone()
    results = {}
    for ii, column in enumerate(rhs_columns):
        results[column] = unique_values[ii]
    return results
        
            
    



def get_triplet_unique_values(cursor, full_table_name, column_name_1, column_name_2, column_name_3, filters=None, count_null_as_distinct_value=True):
    if count_null_as_distinct_value:
        unique_value_query = f"""
        SELECT 
            COUNT( DISTINCT COALESCE({column_name_3}::VARCHAR, 'NULL_PLACEHOLDER') ) as unique_values
        FROM
            {full_table_name}
        """
    else:
        unique_value_query = f"""
        SELECT 
            COUNT( DISTINCT {column_name_3} ) as unique_values
        FROM
            {full_table_name}
        """
    if filters:
        unique_value_query += f"""
        WHERE {filters}
        """
    if count_null_as_distinct_value:
        unique_value_query += f"""
        GROUP BY 
        COALESCE({column_name_1}::VARCHAR, 'NULL_PLACEHOLDER'), 
        COALESCE({column_name_2}::VARCHAR, 'NULL_PLACEHOLDER')
        ORDER BY unique_values DESC
        LIMIT 1
        """
    else:
        unique_value_query += f"""
        GROUP BY {column_name_1}, {column_name_2}
        ORDER BY unique_values DESC
        LIMIT 1
        """
    cursor.execute(unique_value_query)
    unique_values = cursor.fetchone()[0]
    return unique_values
