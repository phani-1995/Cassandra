fi = open("/home/phani/PycharmProjects/Cassandra/project/transactions.csv", 'r')
lines=fi.readlines()

x=lines[0].split(",")

for i in range(1, len(lines)):
	y=lines[i].split(",")
	data={x[0]:y[0],x[1]:y[1],x[2]:y[2],x[3]:y[3],x[4]:y[4],x[5]:y[5],x[6]:y[6],x[7]:y[7],x[8]:y[8],x[9]:y[9],x[10]:y[10],x[11]:y[11],x[12].replace("\n",""):y[12].replace("\n","")}
	fo = open('/home/phani/PycharmProjects/Cassandra/project/jsondata'+str(i)+'.json', 'w+')
	fo.write(str(data))
	fo.close()
fi.close()
