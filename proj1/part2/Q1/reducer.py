import sys
from collections import Counter
from operator import itemgetter

total_count = {}
hit_count = {}
player_pairs = []
players = []
player_def_list = []

for line in sys.stdin:
    line = line.strip()
    pair_hit, count = line.split('\t')
    player, defender, hit = pair_hit.split('_')
    count = int()
    
    pair = player + '_' + defender

    total_count[pair] = total_count.get(pair,0) + count

    if hit == 'made':
        hit_count[pair] = hit_count.get(pair,0) + count
    
    player_pairs.append(pair)
    player_pairs = list(set(player_pairs))
    players.append(player)
    players = list(set(players))


for player in players:
    hitrate = []
    for pair in player_pairs:
        tempP, tempD = pair.split('_')
        if tempP != player:
            continue
        hitrate = float(hit_count.get(pair,0))/float(total_count.get(pair))
        
        hitrate.append([pair,hitrate,total_count.get(pair)])
    sorted_hit_rate = sorted(hitrate,key=lambda x: (x[1],-x[2]))
    
    for select_pair in sorted_hit_rate[0:5]:
        select_player,select_def = select_pair[0].split('_')
        player_def_list.append([player,select_def,select_pair[1],select_pair[2]])


for player, defender, hitrate, shottimes in player_def_list:
    print(player, '--', defender, '--', hitrate, '--', shottimes)
