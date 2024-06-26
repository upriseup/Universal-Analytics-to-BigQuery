NUMBER_OF_GOALS = 10
metrics_goals_1 = [{'expression': f'ga:goal{i+1}Completions'} for i in range(NUMBER_OF_GOALS)]
NUMBER_OF_GOALS_2 = 10
metrics_goals_2 = [{'expression': f'ga:goal{i+11}Completions'} for i in range(NUMBER_OF_GOALS_2)]

for i in metrics_goals_1:
    print(i)

print("finish1")
for i in metrics_goals_2:
    print(i)
print("finish2")


