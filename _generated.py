
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
    
    
    #Generate the code for mfQuery
    selectAttributes = "prod,year,1_avg_quant,2_sum_quant"
    groupingVarCount = 2
    groupingAttributes = "prod,year"
    fVect = "1_avg_quant,2_sum_quant"
    predicates = "1.state = 'NJ' and 1.year = 2016, 2.month = 3 and 2.quant > 1_avg_quant"
    havingCondition = "1_avg_quant > 0.25 * 2_sum_quant"
    print("mfQuery")

    predicates = predicates.split(',')
    pList = []
    #splits predicates by each predicate statment and creates list to store the parts of each predicate in a single 2D array
    for i in predicates:
        pList.append(i.split(' '))
    print(pList)
    rows = list(cursor)

    for i in range(int(groupingVarCount)+1):
        if i == 0:
            for row in rows:
            # creates a key for the dictionary to store the values of the grouping variables
                key = ''
                for attr in groupingAttributes.split(','):
                    key += f'{str(row[attr])},'
                key = key[:-1]
                if key not in MF_Struct:
                    MF_Struct[key] = {}
                    # add the columns of grouping attributes to the dictionary
                    for attr in groupingAttributes.split(','):
                        MF_Struct[key][attr] = row[attr]
                    # add the columns of the aggregate functions to the dictionary
                    for agg in fVect.split(','):
                        if 'sum' in agg:
                            MF_Struct[key][agg] = 0
                        elif 'max' in agg:
                            MF_Struct[key][agg] = 0
                        elif 'min' in agg:
                            MF_Struct[key][agg] = 100000
                        elif 'count' in agg:
                            MF_Struct[key][agg] = 0
                        elif 'avg' in agg:
                            MF_Struct[key][agg] = {'count':0, 'sum':0, 'avg':0}

        else:
            #iterates through each row in the cursor for other grouping variables
            for varAggAttr in fVect.split(','):
                List = varAggAttr.split('_')
                Var = List[0]
                Func = List[1]
                Attr = List[2]
                # check if the grouping variable matches iteration
                if Var == str(i):
                    for row in rows:
                        key = ''
                        for attr in groupingAttributes.split(','):
                            key += f'{str(row[attr])},'
                        key = key[:-1]
                        # try to construct the condition for the row
                        if Func == 'sum':
                            condition = predicates[i-1]
                            for p in pList[i-1]:
                                # check if the predicate is a column in the table
                                if len(p.split('.')) > 1 and p.split('.')[0] == str(i):
                                    rowVal = row[p.split('.')[1]]
                                    try:
                                        int(rowVal)
                                        condition = condition.replace(p, str(rowVal))
                                    except:
                                        condition = condition.replace(p, f"'{rowVal}'")
                                # check if the predicate is an aggregate function
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] in ['sum', 'count', 'min', 'max']:
                                    condition = condition.replace(p, str(MF_Struct[key][p]))
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] == 'avg':
                                    condition = condition.replace(p, str(MF_Struct[key][p]['avg']))
                                # if the predicate is any other operator, ignore it
                                else:
                                    pass
                            condition = condition.replace('=', '==')
                            if eval(condition):
                                MF_Struct[key][varAggAttr] += int(row[Attr])
                        elif Func == 'max':
                            condition = predicates[i-1]
                            for p in pList[i-1]:
                                if len(p.split('.')) > 1 and p.split('.')[0] == str(i):
                                    rowVal = row[p.split('.')[1]]
                                    try:
                                        int(rowVal)
                                        condition = condition.replace(p, str(rowVal))
                                    except:
                                        condition = condition.replace(p, f"'{rowVal}'")
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] in ['sum', 'count', 'min', 'max']:
                                    condition = condition.replace(p, str(MF_Struct[key][p]))
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] == 'avg':
                                    condition = condition.replace(p, str(MF_Struct[key][p]['avg']))
                                else:
                                    pass
                            condition = condition.replace('=', '==')
                            if eval(condition):
                                if int(MF_Struct[key][varAggAttr]) < int(row[Attr]):
                                    MF_Struct[key][varAggAttr] = int(row[Attr])
                        elif Func == 'min':
                            condition = predicates[i-1]
                            for p in pList[i-1]:
                                if len(p.split('.')) > 1 and p.split('.')[0] == str(i):
                                    rowVal = row[p.split('.')[1]]
                                    try:
                                        int(rowVal)
                                        condition = condition.replace(p, str(rowVal))
                                    except:
                                        condition = condition.replace(p, f"'{rowVal}'")
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] in ['sum', 'count', 'min', 'max']:
                                    condition = condition.replace(p, str(MF_Struct[key][p]))
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] == 'avg':
                                    condition = condition.replace(p, str(MF_Struct[key][p]['avg']))
                                else:
                                    pass
                            condition = condition.replace('=', '==')
                            if eval(condition):
                                if int(MF_Struct[key][varAggAttr]) > int(row[Attr]):
                                    MF_Struct[key][varAggAttr] = int(row[Attr])
                        elif Func == 'count':
                            condition = predicates[i-1]
                            for p in pList[i-1]:
                                if len(p.split('.')) > 1 and p.split('.')[0] == str(i):
                                    rowVal = row[p.split('.')[1]]
                                    try:
                                        int(rowVal)
                                        condition = condition.replace(p, str(rowVal))
                                    except:
                                        condition = condition.replace(p, f"'{rowVal}'")
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] in ['sum', 'count', 'min', 'max']:
                                    condition = condition.replace(p, str(MF_Struct[key][p]))
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] == 'avg':
                                    condition = condition.replace(p, str(MF_Struct[key][p]['avg']))
                                else:
                                    pass
                            condition = condition.replace('=', '==')
                            if eval(condition):
                                MF_Struct[key][varAggAttr] += 1
                        elif Func == 'avg':
                            condition = predicates[i-1]
                            for p in pList[i-1]:
                                if len(p.split('.')) > 1 and p.split('.')[0] == str(i):
                                    rowVal = row[p.split('.')[1]]
                                    try:
                                        int(rowVal)
                                        condition = condition.replace(p, str(rowVal))
                                    except:
                                        condition = condition.replace(p, f"'{rowVal}'")
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] in ['sum', 'count', 'min', 'max']:
                                    condition = condition.replace(p, str(MF_Struct[key][p]))
                                elif len(p.split('_'))>1 and int(p.split('_')[0]) and p.split('_')[1] == 'avg':
                                    condition = condition.replace(p, str(MF_Struct[key][p]['avg']))
                                else:
                                    pass
                            condition = condition.replace('=', '==')
                            if eval(condition):
                                MF_Struct[key][varAggAttr]['sum'] += int(row[Attr])
                                MF_Struct[key][varAggAttr]['count'] += 1
                                MF_Struct[key][varAggAttr]['avg'] = MF_Struct[key][varAggAttr]['sum']/MF_Struct[key][varAggAttr]['count']
                                
    print(MF_Struct)
    output = PrettyTable()
    output.field_names = selectAttributes.split(',')
    for row in MF_Struct:
        condition = havingCondition
        if (condition != ""):
            for con in havingCondition.split(' '):
                if con in fVect.split(','):
                    if 'avg' in con:
                        condition = condition.replace(con, str(MF_Struct[row][con]['avg']))
                    else:
                        condition = condition.replace(con, str(MF_Struct[row][con]))
                elif con in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:
                    continue
                else:
                    condition = condition.replace(con, con)
            if eval(condition.replace('=', '==')):
                tuples = []
                for attr in selectAttributes.split(','):
                    if 'avg' in attr:
                        tuples.append(MF_Struct[row][attr]['avg'])
                    else:
                        tuples.append(MF_Struct[row][attr])
                output.add_row(tuples)
        else:
            tuples = []
            for attr in selectAttributes.split(','):
                if 'avg' in attr:
                    tuples.append(MF_Struct[row][attr]['avg'])
                else:
                    tuples.append(MF_Struct[row][attr])
            output.add_row(tuples)
    print(output)
    
    # for row in cur:
    #     if row['quant'] > 10:
    #         _global.append(row)
    
    
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
    