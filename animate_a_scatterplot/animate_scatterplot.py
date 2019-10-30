

import pandas as pd
import matplotlib.pyplot as plt

fert = pd.read_csv('~/git/data/animate_a_scatterplot/gapminder_total_fertility.csv', index_col = 0)

life = pd.read_excel('~/git/data/animate_a_scatterplot/gapminder_lifeexpectancy.xlsx', index_col = 0)

ncol = [int(x) for x in fert.columns]

fert.set_axis(axis=1, labels=ncol, inplace=True)

long_fert = fert.stack()

long_life = life.stack()

my_data = {"fertility" : long_fert, "life_expectancy" : long_life}

new_dataframe = pd.DataFrame(data = my_data)

population = pd.read_excel("~/git/data/animate_a_scatterplot/gapminder_population.xlsx", index_col = 0)

long_population = population.stack()

all_data = {"fertility" : long_fert, "life_expectancy" : long_life, "population" : long_population}

combined_dataset = pd.DataFrame(data = all_data)

df3 = combined_dataset.stack()

df4 = df3.unstack((0,2))

df5 = df3.unstack(2)
df5.plot.scatter('fertility', 'life_expectancy', s=0.1)

df6 = df3.unstack(1)
df6 = df6.unstack(1)
cmap = plt.get_cmap('tab20', lut = len(df6)).colors

import imageio
images = []
for y in range(1960, 2016):
    yearly_data = df6[int(y)]
    yearly_data.plot.scatter('fertility', 'life_expectancy', s = (yearly_data['population'] // 1000000), c = cmap)
    cmap = plt.get_cmap('tab20', lut = len(df6)).colors
    plt.axis([0, 8, 0, 90])
    plt.title(f'Year : {y}') 
    plt.savefig(f'frame_{y}.png')
    im = imageio.imread(f'frame_{y}.png')
    images.append(im)
    plt.figure()
    
imageio.mimsave('fertility_and_life_exp.gif', images, fps = 20)   





