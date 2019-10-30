
import pandas as pd
import numpy as np
import random as rm
import datetime as dt
monday_df = pd.read_csv('/Users/diana/Desktop/wip/week_9/supermarket_data/monday.csv', sep = ";")
tuesday_df = pd.read_csv("/Users/diana/Desktop/wip/week_9/supermarket_data/tuesday.csv", sep = ";")
wednesday_df = pd.read_csv("/Users/diana/Desktop/wip/week_9/supermarket_data/wednesday.csv", sep = ";")
thursday_df = pd.read_csv("/Users/diana/Desktop/wip/week_9/supermarket_data/thursday.csv", sep = ";")
friday_df = pd.read_csv("/Users/diana/Desktop/wip/week_9/supermarket_data/friday.csv", sep = ";")


df = pd.concat([monday_df, tuesday_df, wednesday_df, thursday_df, friday_df], ignore_index=True)

df["date"] = pd.to_datetime(df["timestamp"], unit = 'ns')

df['weekday'] = df['date'].dt.weekday_name
df = df.drop(['timestamp'], axis = 1)


df["customer_unique"] = df["customer_no"].map(str) + df["weekday"]

df_unique = df.drop_duplicates(subset=["customer_unique", "date"]).reset_index(drop = True)
df_unique.set_index("date", inplace = True)

df_unique = df_unique.groupby('customer_unique').resample("1min").ffill()

df_unique = df_unique.reset_index(level=0, drop=True)

df_unique = df_unique.drop(['customer_no'], axis=1)

df_unique = df_unique.reset_index()

df_unique['year'] = df_unique['date'].dt.year
df_unique['month'] = df_unique['date'].dt.month
df_unique['day'] = df_unique['date'].dt.day
df_unique['hour'] = df_unique['date'].dt.hour
df_unique['minute'] = df_unique['date'].dt.minute



df_prices = pd.DataFrame({'location': ['fruit', 'spices', 'dairy', 'drinks', 'checkout'], 'revenue_per_minute' : [4, 3, 5, 6, 0]})

df_unique = df_unique.merge(df_prices, on = 'location')

grouped_by_customer_purchase = df_unique.groupby(["customer_unique"]).sum()


revenue_per_customer = df_unique['revenue_per_minute'].sum() / df_unique['customer_unique'].nunique()

df_per_customer = df_unique.groupby(['customer_unique']).sum()


df_journey = df_unique.sort_values(by=['customer_unique', 'date']).drop(['date', 'weekday', 'minute', 'hour', 'day', 'month', 'year'], axis = 1).reset_index(drop = True)


list_of_values = []
for i in np.unique(df_journey["customer_unique"].values):
    m = range(1, (df_journey["customer_unique"].value_counts()[i]+1))
    for j in m:
        list_of_values.append(j)

df_journey["state"] = list_of_values

df_journey = df_journey.pivot(index = 'customer_unique', columns = 'state', values = 'location')

df_journey['0'] = 'entrance'
cols = list(df_journey.columns)
cols = [cols[-1]] + cols[:-1]
df_journey = df_journey[cols]
df_journey = df_journey.sort_values(by=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51])
df_journey['state_unique'] = df_journey['0'].str.cat(df_journey[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51]], sep=' - ', na_rep = ' ')

df_journey_transitions = df_journey.drop(columns = ['state_unique'], axis = 1).stack()

df_journey_transitions = df_journey_transitions.to_frame()
df_journey_transitions.rename(columns={0:'location'}, inplace=True)
df_journey_transitions['to_location'] = df_journey_transitions.shift(periods=-1)
df_journey_transitions = df_journey_transitions.reset_index(level='state')
df_journey_transitions.drop(columns = ['state'], axis = 1, inplace = True)
df_journey_transitions['transition'] = df_journey_transitions['location'] + '-' + df_journey_transitions['to_location']
df_journey_transitions = df_journey_transitions[df_journey_transitions.to_location != 'entrance']
transitions = df_journey_transitions['transition'].value_counts()
df_journey_states = df_journey_transitions.groupby(['location']).count().drop(columns =['to_location']).rename(columns={'transition':'transition_location'})
df_journey_probabilities = df_journey_transitions.groupby(['location', 'to_location']).count()
df_journey_probabilities = df_journey_probabilities.reset_index('to_location')
df_journey_probabilities = df_journey_probabilities.merge(df_journey_states, on='location')
df_journey_probabilities['probabilities'] = (df_journey_probabilities['transition'] / df_journey_probabilities['transition_location'])
df_journey_probabilities = df_journey_probabilities.drop(columns = ['transition', 'transition_location'])
df_journey_probabilities = df_journey_probabilities.reset_index()
df_journey_probabilities = df_journey_probabilities.set_index(["location", "to_location"])
probability_martix = df_journey_probabilities.unstack(0)['probabilities']
PROB = df_journey_probabilities.unstack(0)['probabilities'].fillna(0.0).to_dict()
LOCATIONS = ['checkout', 'dairy', 'drinks', 'fruit', 'spices']


class Customer:

    def __init__(self):

        self.location = self.get_first()

    def get_first(self):
        return 'entrance'

    @property
    def checkout_out(self):
        if self.location == 'checkout':
            return True
        else:
            return False

    def move(self):
        self.location_list = []
        while self.location != 'checkout':
            new_location = rm.choices(LOCATIONS, list(PROB[self.location].values()))[0]
            self.location = new_location

            #print(self.location)
            self.location_list.append(new_location)
        return self.location_list

    def state(self):
        if self.location == 'checkout':
            self.state = 'churn'
        else:
            self.state = self.location

        return self.state

    def time_spent(self):
        self.time_spent = len(self.location_list)

        return self.time_spent


population = [Customer() for i in range(100)]

list_moves = []
time_spent_list = []

for i in population:
    k = i.move()
    list_moves.append(k)
    time_spent_per_customer = i.time_spent()
    time_spent_list.append(time_spent_per_customer)

population_time_spent = list(zip(list_moves, time_spent_list))

print(population_time_spent)
