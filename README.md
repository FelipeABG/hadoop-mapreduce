# Soccer Championship Analysis with Hadoop

This project analyzes soccer championship data using Hadoop's MapReduce framework. The dataset includes match details, teams, and match outcomes. It is a collection of data about games in the Brazilian soccer championship containing data off all games from 2003 until 2023.

## Dataset Overview

The dataset used for this project includes the following columns:

- **ID**: Unique identifier for each match
- **Round**: The round number in the championship
- **Date**: The date the match took place
- **Hour**: The hour the match started
- **Day**: The day of the week of the match
- **Home Team**: The team playing at home
- **Visiting Team**: The team playing away
- **Home Team Formation**: The formation of the home team
- **Visiting Team Formation**: The formation of the visiting team
- **Home Team Coach**: The coach of the home team
- **Visiting Team Coach**: The coach of the visiting team
- **Winner**: The winner of the match
- **Stadium**: The stadium where the match was held
- **Home Team Score**: The score of the home team
- **Visiting Team Score**: The score of the visiting team
- **Home Team State**: The state or region of the home team
- **Visiting Team State**: The state or region of the visiting team
- **Winning State**: The state of the winning team

## Project Structure
Each file under the *src* folder represents the whole job, including both the *mapper* and *reducer* class, as well as the *driver* class.

## Hadoop Jobs

### 1. Victories Per Team (WinsPerTeam.java)
This job counts the total victories achieved by each team in the period.

### 2. Avarage goals scored at a match (AvarageGoals.java)
This job calculates the avarage goals scored at a match in the period.

