from bokeh.sampledata import us_states
from bokeh.plotting import *
import csv

us_states = us_states.data.copy()

del us_states["HI"]
del us_states["AK"]

# separate latitude and longitude points for the borders
#   of the states.
state_xs = [us_states[code]["lons"] for code in us_states]
state_ys = [us_states[code]["lats"] for code in us_states]

# init figure
p = figure(title="Plotting Points Example: The 5 Largest Cities in Texas",
           toolbar_location="left", plot_width=1100, plot_height=700)

# Draw state lines
p.patches(state_xs, state_ys, fill_alpha=0.0,
    line_color="#884444", line_width=1.5)


x = []
y =[]
with open("scripts/yelp/latlongs-cluster1.csv", "r") as latlongs_file:
    reader = csv.reader(latlongs_file, delimiter=",")
    for row in reader:
        x.append(float(row[3]))
        y.append(float(row[2]))

# Now group these values together into a lists of x (longitude) and y (latitude)
# x = [-97.7431, -96.79, -97.33, -95.36, -98.49]
# y = [30.26, 32.77, 32.75, 29.76, 29.42]

# x= x[:5]
# y = y[:5]

print(x, y)

# The scatter markers
p.circle(x, y, size=6, color='navy', alpha=1)

# output to static HTML file
output_file("cluster1.html")

# show results
show(p)