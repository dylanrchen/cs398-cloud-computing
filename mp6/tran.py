file = open("./amazon_food_reviews.csv","r")
file2 = open("./short.csv","w+")
for i in range(1,1000):
    line = file.readline()
    file2.write(line)