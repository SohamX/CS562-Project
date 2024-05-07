
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from prettytable import PrettyTable
# from databaseConfig import dbconfig  

MF_Struct = {}

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
    
    
    selectAttributes = "cust,prod,avg_quant,max_quant,min_quant"
    groupingAttributes = "cust,prod,month"
    fVect = "avg_quant,max_quant,min_quant,count_quant"
    predicates = "year=2017"
    havingCondition = "max_quant > 800 and avg_quant > 400"

	# Retrieve the results.
    results = cursor.fetchall()
    
    filtered_results = [row for row in results if apply_conditions(row, predicates)]

    for row in filtered_results:
        key = '' #key to store into the MF Struct
        value = {} #value that will store the columns of the MF Struct for the given row
        for attr in groupingAttributes.split(','): #create key out of the grouping attributes of the current row in the table
            key += f'{str(row[attr])},'
        key = key[:-1] #remove trailing comma
        if key not in MF_Struct.keys(): #if the key is not in the MF Struct, create a new entry for the MF Struct
            for groupAttr in groupingAttributes.split(','):
                colVal = row[groupAttr]
                if colVal:
                    value[groupAttr] = colVal
			#loop through the fVects and initalize the values for each aggreagte function being calculated
			#initalize count to 1, sum to the current row's quant value, min and max to the current row's quant value, and average to a dictionary with 3 componenets
            for fVectAttr in fVect.split(','):
                tableCol = fVectAttr.split('_')[1]
                if (fVectAttr.split('_')[0] == 'avg'): 
					#average is stored as a dictionary tracking, sum, count, and average. Each is calculated and stored when the row is updated
                    value[fVectAttr] = {'sum': row[tableCol], 'count': 1, 'avg': row[tableCol]}
                elif (fVectAttr.split('_')[0] == 'count'):
                    value[fVectAttr] = 1
                else:
                    value[fVectAttr] = row[tableCol]
            MF_Struct[key] = value #insert new row into the MFStruct
        else: #row in table already corresponds to an existing entry in the MF Struct, update the existing entry
            for fVectAttr in fVect.split(','):
                tableCol = fVectAttr.split('_')[1]
                if (fVectAttr.split('_')[0] == 'sum'):
                    MF_Struct[key][fVectAttr] += int(row[tableCol]) #Add the quant to the sum for the corresponding row in the MF Struct
                elif (fVectAttr.split('_')[0] == 'avg'):
                    newSum = MF_Struct[key][fVectAttr]['sum'] + int(row[tableCol])
                    newCount = MF_Struct[key][fVectAttr]['count'] + 1
                    MF_Struct[key][fVectAttr] = {'sum': newSum, 'count': newCount, 'avg': newSum / newCount}
                elif (fVectAttr.split('_')[0] == 'count'):
                    MF_Struct[key][fVectAttr] += 1
                elif (fVectAttr.split('_')[0] == 'min'): #check if the row's quant is a new min compared to the corresponding row of the MFStruct
                    if row[tableCol] < MF_Struct[key][fVectAttr]:
                        MF_Struct[key][fVectAttr] = int(row[tableCol])
                else: #check if the row's quant is a new max compared to the corresponding row of the MFStruct
                    if row[tableCol] > MF_Struct[key][fVectAttr]:
                        MF_Struct[key][fVectAttr] = int(row[tableCol])
	#Generate output table(also checks the HAVING condition)
    output = PrettyTable()
    output.field_names = selectAttributes.split(',')
    for row in MF_Struct:
        evalString = ''
        if havingCondition != '':
			#if there is a having condition, loop through each element of the having condition to fill in the correct information into the evalString
			#the eval string will be equal to the having condition, replaced with the values of the variables in question,
			# then evaluated to check if the row of the MFStruct being examined is to be included in the output table
            for string in havingCondition.split(' '):
                if string not in ['>', '<', '==', '=', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:
                    try:
                        int(string)
                        evalString += string
                    except:
                        if len(string.split('_')) > 1 and string.split('_')[0] == 'avg':
                            evalString += str(MF_Struct[row][string]['avg'])
                        else:
                            evalString += str(MF_Struct[row][string])
                else:
                    evalString += f' {string} '
            if eval(evalString.replace('=', '==')):
                row_info = []
                for val in selectAttributes.split(','):
                    if len(val.split('_')) > 1 and val.split('_')[0] == 'avg':
                        row_info += [str(MF_Struct[row][val]['avg'])]
                    else:
                        row_info += [str(MF_Struct[row][val])]
                output.add_row(row_info)
            evalString = ''
        else:
			#there is no having condition, thus every MFStruct row will be in the output table
            row_info = []
            for val in selectAttributes.split(','):
                if len(val.split('_')) > 1 and val.split('_')[0] == 'avg':
                    row_info += [str(MF_Struct[row][val]['avg'])]
                else:
                    row_info += [str(MF_Struct[row][val])]
            output.add_row(row_info)
    print(output)
	
    # for row in cur:
    #     if row['quant'] > 10:
    #         _global.append(row)
    
    
    # return tabulate.tabulate(_global,
    #                     headers="keys", tablefmt="psql")

def apply_conditions(row, conditions):
    if conditions:
        # Split conditions on 'and' and 'or'
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
    