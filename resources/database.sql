create database ctr_db;
use ctr_db;
create table t_adgame_ctr(
 searchTerm String,
 gameId int,
 numShow double,
 numClick double,
 numRemain double
)
partitioned by(dateKey String);
create table t_adgame_ctr_midres(
 udid String,
 searchTerm String,
 gameId int,
 numShow int,
 numClick int,
 numRemain int
)
partitioned by(dateKey String);