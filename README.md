# Anime-Database-and-Dashboards
I wanted to analyze one of my main interests in order to enable more discovery and more understanding of the anime landscape based on the user’s preferences. This was done in SQL for the cleaning portion and then Tableau for the analysis and visualization.

## Data
Sourced from kaggle (https://www.kaggle.com/datasets/calebmwelsh/anilist-anime-dataset) which was itself sourced from AniList through AniList GraphQL API.

The original dataset consisted of 62 columns and 21,919 rows. This data was a slight challenge since it contained many columns with nested JSONs, nulls, and random imputed values such as ‘[]’. However, using those was beyond the scope of this project and would likely be used for other projects involving machine learning or a deeper, more specific analysis.

## Data Engineering (MySQL/SQL)
During the data engineering stage, I created a staging table to give me a table to work with while keeping the original data ready. During the process I removed many unnecessary columns, standardized the data, removed duplicates, converted blanks and values such as ‘[]’ to NULL, and corrected entries where I saw fit. 

I then created the normalized tables with the correct datatypes and lengths ideal for the database’s processing speed. It consisted of 3 separate tables: anime, anime_genre, and genre. More tables could have been made but this was the ideal setup since many anime contain many genres and having multiple genres for each anime being readily available was too important to remove or make simpler. Making other tables was possible however it would have impacted Tableau's performance so I decided against it. 

Only anime_genre has foreign keys and constraints to the anime table. I then populated the tables with the correct data, added some indexes for querying speed, and then it was ready to go to Tableau


## Analytics & Dashboards (Tableau)
Using the Tableau and MySQL connector, I imported the data into Tableau and connected my 3 tables together by the primary key anime_id.

I had two main goals for this project in Tableau and that was to make a summary dashboard of anime and a finder dashboard for users to search through anime that might interest them. The primary colors used were a light blue (#add8e6) and a crimson pink (#dc143c).

Dashboard 1, Anilist Summary Dashboard, consisted of 5 KPIs and 5 charts with a filter section. I wanted a general overview of the Anilist dataset while also trying to find anything interesting. An example is the Popularity by Score chart, a simple one but effective at giving users hidden anime gems with high score and low popularity. Some of the charts are interactive and the filters on the right also help narrow down anything the user wishes to look deeper into.

Dashboard 2, Anime Finder Dashboard, consisted of 1 main scrollable table with 10 filters meant to make it similar to a website where you can scroll through to filter the user’s potential next watch. Each anime has the link to their specific entry on AniList as well as other important data such as the average score, popularity, format, source, etc.

In addition, there are some quality of life features within the dashboards. The tooltips are helpful and clarify what exactly the user is looking at. The AniList icon on the top of both dashboards take you to AniList’s home page. Finally there are the icons next to the dashboard names that take you to the other dashboard. 

## Key Insights
The main insights gained consisted of the following:
The TV format dominates the dataset by a wide margin compared to movies and OVAs
The majority of anime scores cluster between 60–80, with very few extremes
High popularity does not always correlate with high score, revealing many underrated titles as previously mentioned. 
Anime production increases sharply after the 1990s, aligning with global distribution and streaming growth
The comedy genre overtakes all other genres surprisingly by a decent margin.

## Tradeoffs & Limitations
A project does not go without its limitations. I originally wanted to use the image URLs for each anime in the Anime Finder Dashboard, where the user could click on each row and see the main information as well as the image for each anime but it was too much for Tableau to handle so I had to maneuver a little bit.

I also had to temporarily switch to extracts instead of a live MySQL connection because something as simple as dragging the title to the rows section took 1.5 minutes to process. I ended up adding more indexes in SQL and eventually switched back to a live connection where the problem sorted itself out.

I also encountered some difficulty with the datatypes of certain columns when making filters but through a little help with Google and AI, I was able to not only resolve my problems but also learn something new for future projects.

## What I’d do next
If this project were to be extended, here is what I would do next:
Automating a refresh pipeline to keep the data up to date
Tracking user score changes over time
Building a recommendation engine using genres, scores, and popularity
Adding studio-level and seasonal trend analysis
Introducing lightweight ML for similarity-based recommendations
