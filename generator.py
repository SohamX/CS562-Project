# import subprocess
from mfQuery import mfQuery
from sqlQuery import sqlQuery


def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
    # Get the MF SQL structure from the user
    # selectAttributes = "cust,prod,avg_quant,max_quant"
    # groupingVarCount = 0
    # groupingAttributes = "cust,prod"
    # fVect = "avg_quant,max_quant,min_quant,count_quant"
    # predicates = ""
    # havingCondition = ""

    # Receive Input
    inputType = input("Please enter the name of the file which you would like to read, or enter nothing to input the variables inline: ")
    selectAttributes = ""
    groupingVarCount = ""
    groupingAttributes = ""
    fVect = ""
    predicates = ""
    havingCondition = ""

    if inputType != "":
        with open(inputType) as f:
            content = f.readlines()
        content = [x.rstrip() for x in content]
        i = 0
        while i < len(content):
            if(content[i] == "SELECT ATTRIBUTE(S):"):
                i += 1
                selectAttributes = content[i].replace(" ", "")
                i += 1
            elif(content[i] == "NUMBER OF GROUPING VARIABLES(n):"):
                i += 1
                groupingVarCount = content[i].replace(" ", "")
                i += 1
            elif(content[i] == "GROUPING ATTRIBUTES(V):"):
                i += 1
                groupingAttributes = content[i].replace(" ", "")
                i += 1
            elif(content[i] == "F-VECT([F]):"):
                i += 1
                fVect = content[i].replace(" ", "")
                i += 1
            elif(content[i] == "SELECT CONDITION-VECT:"):
                i += 1
                predicates = content[i]
                i += 1
            elif(content[i] == "HAVING_CONDITION(G):"): 
                i += 1     
                havingCondition = content[i]
                i += 1
            else:
                predicates += "," + content[i]
                i += 1
        #trim input of whitespace
        selectAttributes = selectAttributes.replace(" ", "")
        groupingVarCount = groupingVarCount.replace(" ", "")
        groupingAttributes = groupingAttributes.replace(" ", "")
        fVect = fVect.replace(" ", "")
        predicates = predicates #white space needed to evaluate each predicate statment 
        havingCondition = havingCondition #white space needed to evaluate each having condition

    else:
        #read inline
        selectAttributes = input("Please input the select attributes seperated by a comma: ").replace(" ", "")
        groupingVarCount = input("Please input the number of grouping variables: ").replace(" ", "")
        groupingAttributes = input("Please input the grouping attribute(s). If more than one, seperate with commas: ").replace(" ", "")
        fVect = input("Please input the list of aggregate functions for the query seperated by a comma: ").replace(" ", "")
        predicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma. Each predicate must have each element sperated by a space: ")
        havingCondition = input("Please input the having condition with each element seperated by spaces, and the AND and OR written in lowercase: ")

    groupingVarCount = int(groupingVarCount)

    # Get the MFQuery structure from the user
    # selectAttributes = "cust,1_sum_quant,1_avg_quant,2_sum_quant,2_avg_quant,3_sum_quant,3_avg_quant"#"cust,1_max_quant,1_min_quant,1_count_quant" 
    # groupingVarCount = 3#1
    # groupingAttributes = "cust"#"cust"
    # fVect = "1_sum_quant,1_avg_quant,2_sum_quant,2_avg_quant,3_sum_quant,3_avg_quant"#"1_max_quant,1_min_quant,1_count_quant"
    # predicates = "1.state = 'NY',2.state = 'NJ',3.state = 'CT'"#"1.state = 'NY' and 1.year = 2016"
    # havingCondition = "1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant"#""

    #check if mf query or normal sql query
    # if it is a mf query, then generate the code for mf query
    if(groupingVarCount > 0):
        algorithm = mfQuery(selectAttributes, groupingVarCount, groupingAttributes, fVect, predicates, havingCondition)
    else:
        algorithm = sqlQuery(selectAttributes, groupingAttributes, fVect, predicates, havingCondition)
    # if it is a normal sql query, then generate the code for normal sql query

    # Generate the body of the query


    body = f"""
    {algorithm}
    # for row in cur:
    #     if row['quant'] > 10:
    #         _global.append(row)
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from prettytable import PrettyTable
# from databaseConfig import dbconfig  

MF_Struct = {{}}

def query():
    load_dotenv()
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    dbname = os.getenv('DB_NAME')
    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
        cursor_factory=psycopg2.extras.DictCursor)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sales")
    
    # _global = []
    {body}
    
    # return tabulate.tabulate(_global,
    #                     headers="keys", tablefmt="psql")

def apply_conditions(row, conditions):
    if conditions:
        # Splits conditions on 'and' and 'or'
        operators = []
        parts = []
        tmp = conditions
        
        # Identify all operators and split conditions
        while ' and ' in tmp or ' or ' in tmp:
            if ' and ' in tmp:
                pos = tmp.index(' and ')
                parts.append(tmp[:pos])
                operators.append('and')
                tmp = tmp[pos+5:]  # skip ' and '
            elif ' or ' in tmp:
                pos = tmp.index(' or ')
                parts.append(tmp[:pos])
                operators.append('or')
                tmp = tmp[pos+4:]  # skip ' or '
        
        parts.append(tmp)  # add the last or only part
        
        # Evaluate each condition part
        results = []
        for part in parts:
            column, value = part.strip().split('=')
            column = column.strip()
            value = value.strip()
            # Apply each condition to the row and store result
            results.append(str(row[column]) == value)
        
        # Combine results based on operators
        if results:
            result = results[0]
            for op, res in zip(operators, results[1:]):
                if op == 'and':
                    result = result and res
                elif op == 'or':
                    result = result or res
            return result
        return False
    return True  # No condition so process all rows


def main():
    print(query())

if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    # subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
