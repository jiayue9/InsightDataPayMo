import pandas as pd
import numpy  as np
import os


# Read the payment history and extract id pairs 
# Stored them in two lists ID1 and ID2

ID1 = []
ID2 = []
print(os.getcwd())
with open('paymo_input/batch_payment.txt') as f:
    next(f)
    for row in f:
        id1_temp = row.split(',')[1]
        id2_temp = row.split(',')[2]
        ID1.append(int(id1_temp.strip()))
        ID2.append(int(id2_temp.strip()))


print("Read payment history complete")      

# Convert the two lists into a data frame
# Sort in each row, and get unique id pairs

d = {'id1': pd.Series(ID1), 'id2': pd.Series(ID2)}
pairs = pd.DataFrame(d)
pairs = pairs.dropna()
pairs = pairs.apply(np.sort, axis = 1) 
pairs = pairs.drop_duplicates()


print("Preprocessing complete")


def Friends(id):
  id = int(id)
  pairs_with_id = pairs[(pairs.id1 == id) | (pairs.id2 == id)]
  friends_list  = pd.concat([pairs_with_id['id1'], pairs_with_id['id2']]).unique()
  friends_list  = np.delete(friends_list, np.where(friends_list == id))
  return friends_list


def Feature1(id1, id2):
  id1_friends = Friends(id1)
  if id2 in id1_friends:
      result = "trusted"
  else:
      result = "unverified"
  return(result)


def Feature2(id1, id2):
  id1_friends = Friends(id1)
  id2_friends = Friends(id2)
  common_friends = np.intersect1d(id1_friends, id2_friends)
  if common_friends.size != 0:
      result = "trusted"
  else:
      result = "unverified"
  return(result)
      

def Feature3(id1, id2):
  if Feature1(id1, id2) == "trusted":
      return("trusted")
      
  if Feature2(id1, id2) == "trusted":
      return("trusted")

  if Friends(id1).size > Friends(id2).size:
      temp = id1
      id1  = id2
      id2  = temp

  id1_friends_first = Friends(id1)
  id2_friends_first = Friends(id2)
  
  id1_friends_second = np.empty(0)
  for i in range(id1_friends_first.size):
      temp = Friends(id1_friends_first[i])
      common_friends_temp = np.intersect1d(temp, id2_friends_first)
      if common_friends_temp.size != 0:
          return("trusted")
      np.concatenate([id1_friends_second, temp])

  id2_friends_second = np.empty(0)
  for i in range(id2_friends_first.size):
      temp = Friends(id2_friends_first[i])
      common_friends_temp = np.intersect1d(temp, id1_friends_second)
      if common_friends_temp.size != 0:
          return("trusted")
      np.concatenate([id2_friends_second, temp])

  return("unverified")

result1 = []
result2 = []
result3 = []

rownum = 1
with open('paymo_input/stream_payment.txt') as f:
    next(f)
    for row in f:
        rownum = rownum + 1
        id1_temp = int(row.split(',')[1].strip())
        id2_temp = int(row.split(',')[2].strip())
        result1.append(Feature1(id1_temp, id2_temp))
        result2.append(Feature2(id1_temp, id2_temp))
        result3.append(Feature3(id1_temp, id2_temp))
        if rownum % 1000 == 0:
            print(rownum)

print(result1)

f = open('paymo_output/output1.txt','w')
for ele in result1:
    f.write(ele+'\n')
f.close()

f = open('paymo_output/output2.txt','w')
for ele in result2:
    f.write(ele+'\n')
f.close()

f = open('paymo_output/output3.txt','w')
for ele in result3:
    f.write(ele+'\n')
f.close()
