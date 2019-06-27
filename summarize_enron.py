#!/usr/bin/python

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
import collections
import csv
import re

###Data uploading and raw cleaning
# upload the data file
file_name = 'enron-event-history-all.csv'
enron_data = pd.read_csv(file_name, names=['time', 'message identifier', 'sender', 'recipients', 'topic', 'mode'],
                         header=None)

# convert all string to be lower case, because there are same name not in the same format
enron_data = enron_data.applymap(lambda s: s.lower() if type(s) == str else s)
# drop columns after investigation
enron_data = enron_data.drop(columns=['message identifier', 'topic', 'mode'])
# convert time form milliseconds to datetime
enron_data['time'] = enron_data['time'].apply(lambda x: time.gmtime(x / 1000.))
enron_data = enron_data.dropna(axis=0, how='any')
enron_data.head(10)


###Create contact csv file
# functions built up
def clean_email(s1):
    # remove @email address
    s1 = s1.split('@')[0]
    # replace special char with space
    s1 = re.sub('[^A-Za-z0-9]+', ' ', s1)
    # remove all prefix and suffix space
    s1 = s1.strip(' ')
    return s1


def process_person_count(df):
    person_count = {}
    for idx, row in df.iterrows():
        person = clean_email(row[1])

        # sender
        if person not in person_count:
            person_count[person] = [1, 0]
        else:
            # old_count =
            person_count[person] = [person_count[person][0] + 1, person_count[person][1]]

        # recepients
        recepients = row[2].split('|')
        # print(recipients)
        for recepient in recepients:
            recepient = clean_email(recepient)
            if recepient not in person_count:
                person_count[recepient] = [0, 1]
            else:
                person_count[recepient] = [person_count[recepient][0], person_count[recepient][1] + 1]
    return person_count


def write_csv(df):
    # writes the dictionary to the output
    with open('contact.csv', mode='w', newline='') as contact:
        contact_writer = csv.writer(contact, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        contact_writer.writerow(['person', 'sent', 'received'])
        for person in df.index:
            contact_writer.writerow([person, person_count[person][0], person_count[person][1]])


# convert dict to df for later sorting
person_count = process_person_count(enron_data)
df = pd.DataFrame.from_dict(person_count, orient='index', columns=['sent', 'received'])
df.index.names = ['person']
# df.head(10)

# sort by sent
sort_by_sent = df.sort_values('sent', ascending=False)
sort_by_sent.head(10)

# write output
write_csv(sort_by_sent)

###Deal with contacts over time
# get top sender from question 1
topNSender = sort_by_sent.index[0:5]
topNSender

#get the sender over time
# get the time_count
time_count = (2002 - 1998 + 1) * 12

"""
for the time part, before I have tried time based on 6 month range. After trying the plot, I noticed that 
there was almost zero counts before year 2000. So I abandon the data before year 2000 and analyze left data monthly

"""

def get_time():
    yearMonthly = []
    for i in range(time_count):
        year = i // 12 + 1998
        month = i % 12 + 1
        year = str(year)
        if month < 10:
            month = str(0) + str(month)
        else:
            month = str(month)
        yearMonthly.append(year + month)
    return yearMonthly


yearMonthly = get_time()

#get sender over time
def sender_over_time(enron_data):
    SenderCountOverTime = {}
    for row in enron_data.iterrows():
        row_id, content = row
        times, sender, receiver = content
        if sender in topNSender:
            listOfreceiver = receiver.split("|")

            countOverTime = {}
            formatTime = time.strftime('%m/%d/%Y %H:%M:%S', times)
            month, day, yearandtime = formatTime.split("/")
            year, exactTime = yearandtime.split(" ")

            index = (int(year) - 1998) * 12 + int(month)
            if not sender in SenderCountOverTime.keys():
                templist = [0] * time_count
                templist[index] = len(listOfreceiver)
                SenderCountOverTime[sender] = templist[:]
            else:
                templist[index] = SenderCountOverTime[sender][index] + len(listOfreceiver)
                SenderCountOverTime[sender] = templist[:]
    return SenderCountOverTime

#output sender over time
SenderCountOverTime = sender_over_time(enron_data)


# get the receiver over time
def unique_received_count_over_time(enron_data):
    # define the time interval
    yearHafly = {'f1998': 0, 's1998': 1, 'f1999': 2, 's1999': 3, 'f2000': 4, 's2000': 5, 'f2001': 6, 's2001': 7,
                 'f2002': 8, 's2002': 9}

    trackingunique = {}
    uniqueReceivedCountOverTime = {}

    for row in enron_data.iterrows():
        row_id, content = row
        #    print(row_id)
        times, sender, receiver = content
        formatTime = time.strftime('%m/%d/%Y %H:%M:%S', times)
        month, day, yearandtime = formatTime.split("/")
        year, exactTime = yearandtime.split(" ")
        countreceiver = 0
        listOfreceiver = receiver.split("|")
        if int(month) < 6:
            # first half year
            haflyCount = str("f") + str(year)
        else:
            # second half year
            haflyCount = str("s") + str(year)
        index = yearHafly.get(haflyCount)
        while countreceiver < len(listOfreceiver):
            preceiver = listOfreceiver[countreceiver]

            # clean the email to remove other charaters and only keep names
            preceiver = clean_email(preceiver)
            if preceiver in topNSender:
                timeAndSender = str(preceiver) + str(sender) + str(haflyCount)
                if not timeAndSender in trackingunique.keys():
                    trackingunique[timeAndSender] = 1
                    if not preceiver in uniqueReceivedCountOverTime.keys():
                        tempor = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                        tempor[index] = 1
                        uniqueReceivedCountOverTime[preceiver] = tempor
                    else:
                        uniqueReceivedCountOverTime[preceiver][index] += 1
                else:
                    trackingunique[timeAndSender] = 1
            countreceiver += 1

    return uniqueReceivedCountOverTime

#output receiver output
uniqueReceivedCountOverTime = unique_received_count_over_time(enron_data)

#convert keys and values of dictionary to be lists
topNPersonSend = list(SenderCountOverTime.keys())
topNPersonSendCount = list(SenderCountOverTime.values())
topNPersonReceived = list(uniqueReceivedCountOverTime.keys())
topNPersonReceivedCount = list(uniqueReceivedCountOverTime.values())

#plot sending count
sendingFigure = plt.figure("Sending Email Count Monthly")
plt.figure(1, figsize = (8.5,11))
axes = plt.gca()
plt.xticks(range(time_count), yearMonthly, rotation=45)
axes.set_xlim([37,50])
plt.plot(range(time_count), topNPersonSendCount[0], 'b-', label = topNPersonReceived[0])
plt.plot(range(time_count), topNPersonSendCount[1], 'r-', label = topNPersonReceived[1])
plt.plot(range(time_count), topNPersonSendCount[2], 'g-', label = topNPersonReceived[2])
plt.plot(range(time_count), topNPersonSendCount[3], 'y-', label = topNPersonReceived[3])
plt.plot(range(time_count), topNPersonSendCount[4], 'm-', label = topNPersonReceived[4])
plt.legend()
plt.xlabel('Duration(Monthly)')
plt.ylabel('Sending Count')


#save the pics
sendingFigure.savefig("Sending Email Count Monthly", dpi=100)



#set the time interval names
xAxisName = ['1998/1st','1998/2nd','1999/1st','1999/2nd','2000/1st','2000/2nd','2001/1st','2001/2nd','2002/1st','2002/2nd']
#plot receiving count
receivingFigure = plt.figure("Unique Email Receiving Count For Each Six Months")
plt.figure(2, figsize = (10,14))
axes = plt.gca()
axes.set_xlim([0,11])
axes.set_ylim([0,300])
yticks = np.arange(0, 300, 20)
plt.xticks(range(10), xAxisName, rotation = 45)
plt.yticks(yticks)
plt.plot(range(10), topNPersonReceivedCount[0], 'b-', label = topNPersonReceived[0])
plt.plot(range(10), topNPersonReceivedCount[1], 'r-', label = topNPersonReceived[1])
plt.plot(range(10), topNPersonReceivedCount[2], 'g-', label = topNPersonReceived[2])
plt.plot(range(10), topNPersonReceivedCount[3], 'y-', label = topNPersonReceived[3])
plt.plot(range(10), topNPersonReceivedCount[4], 'm-', label = topNPersonReceived[4])
plt.legend()
plt.xlabel('Duration(Six Months)')
plt.ylabel('Receiving Count')


#save the pics
receivingFigure.savefig("Unique Email Receiving Count For Each Six Months", dpi=100)

