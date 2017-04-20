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
sessionid string,
searchterm String,
querytimestamp String,
gameid int,
numshow int,
numclick int,
numremain int
)
partitioned by(dateKey String);