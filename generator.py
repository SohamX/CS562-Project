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
            elif(content[i] == "SELECT CONDITION-VECT([σ]):"):
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
    # selectAttributes = "cust,1_max_quant,1_min_quant,1_count_quant" 
    # groupingVarCount = 1
    # groupingAttributes = "cust"
    # fVect = "1_max_quant,1_min_quant,1_count_quant"
    # predicates = "1.state = 'NY' and 1.year = 1992"
    # havingCondition = ""

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
import psycopg2
import psycopg2.extras
from databaseConfig import dbconfig
from sqlQuery import sqlQuery    

MF_Struct = {{}}

def query():
    {body}

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