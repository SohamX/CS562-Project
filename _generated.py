
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
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')
    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
        cursor_factory=psycopg2.extras.DictCursor)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sales")
    
    # _global = []
    
    
    #Generate the code for mfQuery
    selectAttributes = "cust,prod,1_avg_quant,2_avg_quant"
    groupingVarCount = 2
    groupingAttributes = "cust,prod"
    fVect = "1_avg_quant,2_avg_quant"
    predicates = "1.state = 'NJ',2.quant > 1_avg_quant"
    havingCondition = ""
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

def main():
    print(query())

if "__main__" == __name__:
    main()
    