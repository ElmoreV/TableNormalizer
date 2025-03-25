import snowflake_utils as sf
from pprint import pprint
import pandas as pd
import fdep_utils as fu


cur = sf.get_cursor(conn)
# database = 
# schema =
# table = 

columns = sf.get_columns(cur, database, schema, table)

print(columns)

nullary_results = []
full_table_name = f"{database}.{schema}.{table}"
# filters= "customer_id = 'cu-13c4cf2805f6de814dfcefc1af3be679'"
filters = None
distinct_values = sf.get_distinct_values(cur, full_table_name, filters)
nullary_results.append([distinct_values])
print(distinct_values)

unary_values = sf.get_general_unique_values(cur, full_table_name, [], columns, filters,count_null_as_distinct_value=False)
unary_values = {k:unary_values[k] for k in sorted(unary_values, key=lambda x: unary_values[x], reverse=True)}
unary_values

# Separate out the singletons and the key columns
rest_columns,singletons,  = fu.filter_singletons(unary_values)
rest_columns, key_columns = fu.filter_keys(rest_columns, distinct_values)
print(singletons)
print(key_columns)
print(rest_columns)

print("Singletons:")
for singleton in singletons:
    print(singleton)
print()
print("Keys/Unique Values:")
for key_column in key_columns:
    print(key_column)
print()
# filter out all trivial values (singletons and fully unique from the df)
non_trivial_columns = [[k,v] for k,v in rest_columns.items()]
print("Non trivial columns")
for column in non_trivial_columns:
    print(column[0], column[1])
    
binary_results = []
print(len(non_trivial_columns))
for ii,(col1,col1_val) in enumerate(non_trivial_columns):
    print(ii, col1, col1_val)
    
    # So if col1 -> col2, then |col1| >= |col2|
    # so if there are 20 different values in col2, and only 3 in col1,
    # there by the pigeonhole principle, col1 cannot uniquely determine col2 valuess
    # and I want max(Table.groupby(col1).count(col2))==1, means that there is one col2 for every col1 value
    # filtered_columns = [col2 for (col2,col2_val) in non_trivial_columns if col2 != col1 and col1_val>=col2_val]
    
    #without weird filters
    filtered_columns = [col2 for (col2,col2_val) in non_trivial_columns if col2 != col1]
    if len(filtered_columns) == 0 :
        continue
    
    max_unique_values = sf.get_general_unique_values(cur,
                                                     full_table_name, 
                                                     [col1], 
                                                     filtered_columns,
                                                     filters = filters,
                                                     count_null_as_distinct_value=False
                                                     )
                                                     
    for col2, max_unique_value in max_unique_values.items():
        binary_results.append((col1, col2, max_unique_value))

# print(binary_results)

# for (col1, col2, max_unique_value) in binary_results:
#     print(col1, col2, max_unique_value)
df =  pd.DataFrame(binary_results, columns=["Column1", "Column2", "UniqueMappings"])

print(df)

results = []
num_pairs = len(non_trivial_columns)*(len(non_trivial_columns)-1)
print(f"Number of pairs: {num_pairs}")
for col1 in non_trivial_columns:
    for col2 in non_trivial_columns:
        print(".",end="")
        if col1 == col2:
            continue
        max_unique = sf.get_pairwise_unique_values(cur, full_table_name, col1, col2, filters = filters)
        results.append((col1, col2, max_unique))
        
# Max unique values of column 2 for each value column 1 
# So if we know the value of column 1, we know that there are at most max_unique values of column 2 for that value
df = pd.DataFrame(results, columns=["Column1", "Column2", "UniqueMappings"])
print(df)


df.to_csv("name_name_name.csv")


for row,a in df.iterrows():
    print(a.UniqueMappings)
    
    
# Get trivial columns
# 1. all columns that have 1 single value for the entire table

# 2. all columns that have unique values for every row


non_trivial_columns
df


isomorphic_columns = []
fd_columns = []
non_fd_columns = []
for ii,(col1,col1_val) in enumerate(non_trivial_columns):
    for jj,(col2,col2_val) in enumerate(non_trivial_columns):
        if col1 == col2:
            continue
        df_col1col2 = df.loc[(df['Column1'] == col1) & (df['Column2'] == col2), 'UniqueMappings']
        # df_col2col1 = df.loc[(df['Column1'] == col2) & (df['Column2'] == col1), 'UniqueMappings']
        
        if (len(df_col1col2) == 0):
            continue
        
        print(f"Col {col1} -> {col2}: {df_col1col2.iloc[0]}")
        
        
        
isomorphic_columns = []
fd_columns = []
non_fd_columns = []
for ii,(col1,col1_val) in enumerate(non_trivial_columns):
    for jj,(col2,col2_val) in enumerate(non_trivial_columns):
        if col1 == col2:
            continue
        df_col1col2 = df.loc[(df['Column1'] == col1) & (df['Column2'] == col2), 'UniqueMappings']
        df_col2col1 = df.loc[(df['Column1'] == col2) & (df['Column2'] == col1), 'UniqueMappings']
        
        if (len(df_col1col2) == 0 or len(df_col2col1) == 0):
            continue
        
        # print(f"Col {col1} -> {col2}: {df_col1col2.iloc[0]} vs {df_col2col1.iloc[0]}")
        if (df_col1col2.iloc[0] == 1 
            and df_col2col1.iloc[0] == 1):
            print(f"Column {col1} is bijective/1:1 with {col2}")
            isomorphic_columns.append([col1,col2])
        elif (df_col1col2.iloc[0] == 1 and 
            df_col2col1.iloc[0] >1):
            print(f"Column {col1} is 1:{df_col2col1.iloc[0]} with {col2}")
            fd_columns.append([col1,col2])
            # print(f"This means, for each value of {col1}, the {col2} is uniquely defined.")
        elif (df_col1col2.iloc[0] >1 and 
            df_col2col1.iloc[0] == 1):
            print(f"Column {col2} is 1:{df_col1col2.iloc[0]} with {col1}")
            fd_columns.append([col1,col2])
        elif (df_col1col2.iloc[0] >1 and 
            df_col2col1.iloc[0] > 1):
            print(f"Column {col1} is {df_col1col2.iloc[0]}:{df_col2col1.iloc[0]} with {col2}")
            non_fd_columns.append([col1,col2,df_col1col2.iloc[0],df_col2col1.iloc[0]])


non_fd_columns_2 = sorted(non_fd_columns, key=lambda x: min(x[2],x[3]),reverse=False)
for x in non_fd_columns_2:
    # print(x)
    if (x[2]<x[3]):
        print(f"Column {x[0]} is {x[2]}:{x[3]} with {x[1]}")
    else:
        print(f"Column {x[1]} is {x[3]}:{x[2]} with {x[0]}")
            
print(isomorphic_columns)

for col in columns:
    if col in singletons:
        continue
    if col in key_columns:
        continue
    if col in [y for x in isomorphic_columns for y in x]:
        continue
    if col in [y for x in fd_columns for y in x]:
        print(col)
        continue
    # print(col)
    
# Now I would need to collect all pairs of columns, and then
# check for duplicates with the pairs

columns_to_pair = ["#columns_to_pair"]
ternary_results = []
num_triplets = (len(columns_to_pair)*(len(columns_to_pair)-1))/2*(len(columns_to_pair)-2)
print(f"There are {num_triplets} triplets to check")
n =0 
for ii,col1 in enumerate(columns_to_pair):
    for jj,col2 in enumerate(columns_to_pair):
        if jj<=ii:
            # Checking AB -> C is the same as checking BA -> C
            continue
        for col3 in columns_to_pair:
            if col3 == col2:
                continue
            if col3 == col1:
                continue
            # print(n,col1,col2,col3)
            max_unique = sf.get_triplet_unique_values(cur, full_table_name, col1, col2, col3,filters=filters)
            print(n,col1,col2,col3,max_unique)
            # break
            ternary_results.append((col1, col2, col3, max_unique))
            n+=1
ternary_df = pd.DataFrame(ternary_results, columns=["ColumnLHS1","ColumnLHS2","ColumnRHS1","UniqueMappings"])
    
    
for ii,row in ternary_df.iterrows():
    if row.UniqueMappings == 1:
       print(f"{row.ColumnLHS1},{row.ColumnLHS2}-> {row.ColumnRHS1}")
       
       
for ii,row in ternary_df.iterrows():
    if row.UniqueMappings == 1:
       print(f"{row.ColumnLHS1},{row.ColumnLHS2}-> {row.ColumnRHS1}")
       
       
# the goal is now to know for every possible set of attributes/columns
# ["EUR_DISCOUNT","EUR_NET_REVENUE","PRODUCT_ID","INVOICE_DATE","INVOICE_ID","INVOICE_LINE_START_DATE","CONTRACT_ID"]
# So we now know
# 2->1->0
# 5-4->3

# the results are given as 
# 001000 -> 000001 ? +-1/0
# where +1 means true,
# -1 means false
# 0 means: don't know (yet)
from collections import OrderedDict

N_attributes = len(non_trivial_columns)
# known_fds = ??

# known_fds = [(2,1),(1,0),(5,4),(4,3),(3,2)]

N_attributes = 7
powerset_size = 2**N_attributes
lhs = range(1,powerset_size) #0 and 2^N exactly are trivial.
rhs = range(N_attributes)
fd_map = OrderedDict()

# Different systems here
# 1. minimalize current knowledge (so minimum amount of relations to describe full system)
# 2. find ALL currently known relations (by all Armstrongs rules)
# 3. find ALL currently known non-relations
# 4. find all unknown relations




for a in lhs:
    for b in rhs:
        # print(a,b)
        fd_map[(a,b)] = 0
        # is b subset of a?
        if a & 2<<b > 0:
            fd_map[(a,b)] = 1
        for (c,d) in known_fds:
            # c=2, d=1 means 3->4, so 0000100 (one hot binary for 3) -> 0100 (4 in base 2)
            fd_map[(2<<c,d)]=1
            # but also every superset C of c will have C->d 
           


def find_closure(fd_map, a):
    closure = set()
    for b in rhs:
        if fd_map[(a,b)] == 1:
            closure.add(b)
            extended_closure = find_closure(fd_map, b)
            return 
    
     
cl_map = {}
for a in lhs:
    for b in rhs:
        # calculate closures            
        

for (a,b) in fd_map:        
    print(f"{a:08b}->{2<<b:08b} ? {fd_map[(a,b)]}")

# We need to now do the following:
# 1. Augment the FDs
# 2. Transitivity applied on the FDs.
# 3. Apply inverse composition on the FDs.




import fdep_utils as fu
import importlib
importlib.reload(fu)
exactly_one_bit_set = fu.exactly_one_bit_set
convert_from_bitmap = fu.convert_from_bitmap
convert_to_bitmap = fu.convert_to_bitmap
N_lhs = 2**N_attributes
N_rhs = 2**N_attributes

# [lhs,rhs] -> (-1=false,0=unknown,1=true)
full_fd_map = OrderedDict()


known_fds = [(2,1),(1,0),(5,4),(4,3),(3,2)]

full_fd_map = fu.setup_hashmap(N_attributes)
full_fd_map = fu.insert_known_pairwise_fds(full_fd_map, known_fds,N_attributes)

for ii in range(3):

    full_fd_map = fu.apply_union(full_fd_map, N_attributes)
    full_fd_map = fu.apply_augmentation(full_fd_map, N_attributes)
    full_fd_map = fu.apply_transitivity(full_fd_map, N_attributes)

    print("Known FDs")
    print(len([x for x,v in full_fd_map.items() if v==1]))
    print("Known non-FDs")
    print(len([x for x,v in full_fd_map.items() if v==-1]))
    print("Unknown FDs")
    print(len([x for x,v in full_fd_map.items() if v==0]))                    
    
print("Not Done")


print("Known FDs")
print(len([x for x,v in full_fd_map.items() if v==1]))
print("Known non-FDs")
print(len([x for x,v in full_fd_map.items() if v==-1]))
print("Unknown FDs")
print(len([x for x,v in full_fd_map.items() if v==0]))



missing_sets = set()
for lhs in range(1,N_lhs):
    for rhs in range(1,N_rhs):
        if full_fd_map[(lhs,rhs)] == 0:
            # print(convert_from_bitmap(l),convert_from_bitmap(r))
            for rr in convert_from_bitmap(rhs,N_attributes):
                # print((str(convert_from_bitmap(l)),rr))
                missing_sets.add((str(convert_from_bitmap(lhs,N_attributes)),rr))
# print(missing_sets)
for s in missing_sets:
    print(s[0])
    print((list(s[0]),s[1]))
# print(missing_sets)
print(len(missing_sets))