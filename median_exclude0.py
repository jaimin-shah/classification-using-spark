import csv

ch0 = list()
ch1 = list()
ch2 = list()
ch3 = list()
ch4 = list()
ch5 = list()
ch6 = list()
ch7 = list()

def median(list1):
    lis1 = sorted(list1)
    cnt = len(lis1)
    med = 0
    if cnt % 2 == 0 :
        med = lis1[ ( cnt // 2 ) ] + lis1[ ( cnt // 2 ) - 1 ]
        med = med / 2

    else :
        med = lis1[ (cnt - 1) // 2]

    return med


def calc(lis):
    lenlis = 0
    for i in range(768):
        if lis[i] != 0:
            lenlis += 1

    return lenlis

with open('diabetes.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    j = 0
    lis = list()

    se = set()
    i = 0
    avg = list()
    lis0 = list()
    lis1 = list()
    lis2 = list()
    lis3 = list()
    lis4 = list()
    lis5 = list()
    lis6 = list()
    lis7 = list()
    lis8 = list()

    for row in spamreader:
        lis0.append(int(row[0]))
        lis1.append(int(row[1]))
        lis2.append(int(row[2]))
        lis3.append(int(row[3]))
        lis4.append(int(row[4]))
        lis5.append(float(row[5]))
        lis6.append(float(row[6]))
        lis7.append(int(row[7]))
        lis8.append(int(row[8]))


    




    avg.append(median(lis0))

    avg.append(median(lis1))

    avg.append(median(lis2))

    avg.append(median(lis3))

    avg.append(median(lis4))

    avg.append(median(lis5))

    avg.append(median(lis6))

    avg.append(median(lis7))


    for cnt in range(len(lis0)):
        if lis0[cnt] == 0:
            ch0.append(avg[0])
        else:
            ch0.append(lis0[cnt])

    for cnt in range(len(lis1)):
        if lis1[cnt] == 0:
            ch1.append(avg[1])
        else:
            ch1.append(lis1[cnt])

    for cnt in range(len(lis2)):
        if lis2[cnt] == 0:
            ch2.append(avg[2])
        else:
            ch2.append(lis2[cnt])

    for cnt in range(len(lis3)):
        if lis3[cnt] == 0:
            ch3.append(avg[3])
        else:
            ch3.append(lis3[cnt])

    for cnt in range(len(lis4)):
        if lis4[cnt] == 0:
            ch4.append(avg[4])
        else:
            ch4.append(lis4[cnt])

    for cnt in range(len(lis5)):
        if lis5[cnt] == 0:
            ch5.append(avg[5])
        else:
            ch5.append(lis5[cnt])

    for cnt in range(len(lis6)):
        if lis6[cnt] == 0:
            ch6.append(avg[6])
        else:
            ch6.append(lis6[cnt])

    for cnt in range(len(lis7)):
        if lis7[cnt] == 0:
            ch7.append(avg[7])
        else:
            ch7.append(lis7[cnt])
#print(lis8)
with open('exclude0_median.csv', 'w') as csvopfile:
    colname = ['r0', 'r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7', 'r8']
    writer = csv.DictWriter(csvopfile, colname,lineterminator='\n')
    for x in range(len(ch0)):
        writer.writerow(
            {'r0': ch0[x], 'r1': ch1[x], 'r2': ch2[x], 'r3': ch3[x], 'r4': ch4[x], 'r5': ch5[x], 'r6': ch6[x], 'r7': ch7[x], 'r8': lis8[x]})
