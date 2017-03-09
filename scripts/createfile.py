target = open("sleep0.txt", 'w')
for i in range (0, 1000000):
	target.write("sleep 0")
	target.write("\n")
target.close()
