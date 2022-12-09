import pandas as pd
import mysql.connector
import requests
import json
# this has the mapping of the old and new indexes.
id_key_pair={}
all_old_id=[]
# this has the mapping of the oldID and Name.
id_name_pair={}
CREATE_GROUP_API="http://0.0.0.0:9003/migration/groups"
CREATE_MEMBER_API="http://0.0.0.0:9003/migration/group-members"
CREATE_RULE_API="http://0.0.0.0:9003/migration/rules"

connector = mysql.connector.connect(user='root', password='root',
                              host='127.0.0.1',
                              database='open')
group_query = """select * from maker_checker_groups where is_active=1;"""
group_df = pd.read_sql(group_query,connector)

# following through all the records. Ingestion of Maker_checker_group to Approval_group


for x in range(len(group_df.index)):
    old_id = group_df.at[x, 'maker_checker_groups_id']
    group_name = group_df.at[x, 'group_identifier']
    account_id = group_df.at[x, 'accounts_id']
    company_id = group_df.at[x, 'companies_id']
    user_id = group_df.at[x, 'users_id']
    data = {
            "name": group_name,
            "description": "this is",
            "users_id": int(user_id),
            "accounts_id": int(account_id),
            "companies_id": int(company_id)
        }
    # Making the post request
    response = requests.post(url=CREATE_GROUP_API, json=data)
    json_response = json.loads(response.text)
    # Mapping the old group ID with new UUID
    if response.status_code == 201:
        id_key_pair[old_id]= json_response['id']
        id_name_pair[old_id] = group_name
        all_old_id.append(old_id)
    else:
        print(old_id," Ingestion not successful for group")

print(id_key_pair)
print(id_name_pair)
# adding all the member in the group, based on the user ID.
member_query = """select * from banking_mop_user_settings where is_checker = 1;"""
member_df = pd.read_sql(member_query,connector)

for x in range(len(member_df.index)):
    if int(member_df.at[x, 'maker_checker_groups_id']) in all_old_id:   
        user_id = member_df.at[x, 'users_id']
        table_id=str(member_df.at[x, 'banking_mop_settings_id'])
        account_id = member_df.at[x, 'accounts_id']
        company_id = member_df.at[x, 'companies_id']
        group_id = member_df.at[x, 'maker_checker_groups_id']
        data ={
            "group_id":id_key_pair[int(group_id)],
            "members": [int(user_id)],
            "users_id":int(user_id),
            "accounts_id": int(account_id),
            "companies_id": int(company_id)
        }
        response = requests.post(url=CREATE_MEMBER_API, json=data)
        json_response = json.loads(response.text)
        if response.status_code == 201:
            # print(json_response["group_id"])
            # print("=====================================")
            print(table_id,"Successfully Group members created")
        else:
            print(data, json_response["message"])
            # print("______________________________________")
            print(table_id,"Ingestion not successful group members")


rule_query = """select * from maker_checker_parameters;"""
rule_df = pd.read_sql(rule_query,connector)

for x in range(len(member_df.index)):
    approval_criteria =[]
    conjuntion ="AND"
    user_id = rule_df.at[x, 'users_id']
    account_id = rule_df.at[x, 'accounts_id']
    company_id = rule_df.at[x, 'companies_id']
    amount_range_start = rule_df.at[x, 'amount_range_start']
    amount_range_end = rule_df.at[x, 'amount_range_end']
    approval_rule = rule_df.at[x,'parameter_rule']
    a= str(approval_rule)
    approval_criteria =[]
    conjuntion="AND"
    b=[]
    ANDflag =0;
    if "+" in a:
        ANDflag=1
        b=a.split("+")
    elif ANDflag ==0 and "," in a:    
            b=a.split(",")
    else:
        print("a:",a)
        value= a.split(":")
        single={
            "container_type": "CRITERION",
            "condition": {
            "condition_type": "group",
            "group_name": id_name_pair[int(value[0])],
            "number_of_approvals_required": int(value[1])
            }
        }
        approval_criteria.append(single)
    if b:
        print("b:",b)
        for i in b:
            q={
                "container_type": "CRITERION",
                "condition": {
                "condition_type": "group",
                "group_name": "",
                "number_of_approvals_required": 0
                }
            }
            c=[]
            if "," in i:
                list=[]
                level2={
                    "container_type": "CRITERION_GROUP",
                    "rule_content": {
                        "conjunction": "OR",
                        "criteria": []
                        }
                }
                c=i.split(",")
                for j in c:
                    q={
                        "container_type": "CRITERION",
                        "condition": {
                        "condition_type": "group",
                        "group_name": "",
                        "number_of_approvals_required": 0
                        }
                    }
                    value=j.split(":")
                    q["condition"]["group_name"] = id_name_pair[int(value[0])]
                    q["condition"]["number_of_approvals_required"]=int(value[1])
                    list.append(q)
                level2["rule_content"]["criteria"]=list
                approval_criteria.append(level2)
            else:
                value=i.split(":")
                q["condition"]["group_name"] = id_name_pair[int(value[0])]
                q["condition"]["number_of_approvals_required"]=int(value[1])
                approval_criteria.append(q)
    print(approval_criteria)
    if b and not ANDflag:
        conjuntion ="OR"
    data={
            "rule_metadata":{
                "domain":"Payout"
            },
            "rule_container":{
                "container_type":"CRITERION_GROUP",
                "rule_content":{
                    "conjunction":"AND",
                    "criteria":[
                    {   
                        "container_type": "CRITERION",
                        "condition":{
                        "operand":"amount",
                        "value": str(amount_range_start),
                        "operator":"GREATER_THAN"
                        }
                    },
                    {
                        "container_type": "CRITERION",
                        "condition":{
                        "operand":"amount",
                        "value": str(amount_range_end),
                        "operator":"LESS_THAN"   
                        }
                    }
                    ]
                }
            },
            "approval_container":{
                "container_type":"CRITERION_GROUP",
                "rule_content":{
                    "conjunction":conjuntion,
                    "criteria":approval_criteria
                }    
            },
            "users_id":int(user_id),
            "accounts_id": int(account_id),
            "companies_id": int(company_id)
    }
    print("data:",data)


    # Making the post request for Rule 
    response = requests.post(url=CREATE_RULE_API, json=data)
    json_response = json.loads(response.text)
    if response.status_code == 201:
        print(json_response['id'],"    Successfully Rule members created")
    else:
        print(json_response['message'], "  The Rule hasn't not created")