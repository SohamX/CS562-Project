def mfQuery(selectAttributes, groupingVarCount, groupingAttributes, fVect, predicates, havingCondition):
    # Generate the code for mfQuery
    return f"""
    #Generate the code for mfQuery
    selectAttributes = "{selectAttributes}"
    groupingVarCount = {groupingVarCount}
    groupingAttributes = "{groupingAttributes}"
    fVect = "{fVect}"
    predicates = "{predicates}"
    havingCondition = "{havingCondition}"
    print("mfQuery")

    predicates = predicates.split(',')
    pList = []
    #splits predicates by each predicate statment and creates list to store the parts of each predicate in a single 2D array
    for i in predicates:
        pList.append(i.split(' '))
    for i in range(int(groupingVarCount)+1):
	
        if i == 0:
            for row in cur:
                key = ''
                value = {{}}
                for attr in groupingAttributes.split(','):
                    key += f'{{str(row[attr])}},'
                key = key[:-1]
                if key not in MF_Struct:
                    MF_Struct[key] = {{}}
                    for attr in groupingAttributes.split(','):
                        MF_Struct[key][attr] = row[attr]
                    for agg in fVect.split(','):
                        if 'sum' in agg:
                            MF_Struct[key]['sum'] = 0
                        if 'max' in agg:
                            MF_Struct[key]['max'] = 0
                        if 'min' in agg:
                            MF_Struct[key]['min'] = 100000
                        if 'count' in agg:
                            MF_Struct[key]['count'] = 0
                        if 'avg' in agg:
                            MF_Struct[key]['avg'] = 0
                            MF_Struct[key]['sum'] = 0
                            MF_Struct[key]['count'] = 0

                # for p in pList:
                #     if p[1] == 'sum':
                #         MF_Struct[key]['sum'] += row[p[0]]
                #     if p[1] == 'max':
                #         if row[p[0]] > MF_Struct[key]['max']:
                #             MF_Struct[key]['max'] = row[p[0]]
                #     if p[1] == 'min':
                #         if row[p[0]] < MF_Struct[key]['min']:
                #             MF_Struct[key]['min'] = row[p[0]]
                #     if p[1] == 'count':
                #         MF_Struct[key]['count'] += 1
                #     if p[1] == 'avg':
                #         MF_Struct[key]['sum'] += row[p[0]]
                #         MF_Struct[key]['count'] += 1
                #         MF_Struct[key]['avg'] = MF_Struct[key]['sum'] / MF_Struct[key]['count']
    print(MF_Struct)
    """