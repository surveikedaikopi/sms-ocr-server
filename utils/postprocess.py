import json
import requests
import pandas as pd

from config.config import *


# Functions to fetch and save quick count results
def fetch_quickcount():

    res = requests.get(url_get_event_ids, headers=headers)
    out = res.json()['response']
    
    list_event_id = out['list_events']
    list_event_type = out['list_types']

    data = []
    total_votes_data = {}

    for event_id, event_type in zip(list_event_id, list_event_type):
        params = {'Event ID': event_id}
        if event_type == 'Pilpres':
            url_votes = url_votes_agg_pilpres
        elif event_type == 'Pilgub':
            url_votes = url_votes_agg_provinsi
        elif event_type in ['Pilwalkot', 'Pilbup']:
            url_votes = url_votes_agg_kabkota

        res = requests.get(url_votes, headers=headers, params=params)
        out = res.json()['response']

        regions = out['regions']
        vote1 = out['vote 1']
        vote2 = out['vote 2']
        vote3 = out['vote 3']
        vote4 = out['vote 4']
        vote5 = out['vote 5']
        vote6 = out['vote 6']

        total_votes = [0, 0, 0, 0, 0, 0]
        total_regions_votes = 0

        for region, v1, v2, v3, v4, v5, v6 in zip(regions, vote1, vote2, vote3, vote4, vote5, vote6):
            region_total_votes = v1 + v2 + v3 + v4 + v5 + v6
            total_regions_votes += region_total_votes
            total_votes[0] += v1
            total_votes[1] += v2
            total_votes[2] += v3
            total_votes[3] += v4
            total_votes[4] += v5
            total_votes[5] += v6

            if region_total_votes > 0:
                v1_pct = v1 / region_total_votes * 100
                v2_pct = v2 / region_total_votes * 100
                v3_pct = v3 / region_total_votes * 100
                v4_pct = v4 / region_total_votes * 100
                v5_pct = v5 / region_total_votes * 100
                v6_pct = v6 / region_total_votes * 100
            else:
                v1_pct = v2_pct = v3_pct = v4_pct = v5_pct = v6_pct = 0

            data.append({
                'event_id': event_id,
                'region': region,
                'vote1_pct': v1_pct,
                'vote2_pct': v2_pct,
                'vote3_pct': v3_pct,
                'vote4_pct': v4_pct,
                'vote5_pct': v5_pct,
                'vote6_pct': v6_pct
            })

        if total_regions_votes > 0:
            total_votes_pct = [v / total_regions_votes * 100 for v in total_votes]
        else:
            total_votes_pct = [0, 0, 0, 0, 0, 0]

        total_votes_data[event_id] = {
            'event_id': event_id,
            'region': 'All',
            'vote1_pct': total_votes_pct[0],
            'vote2_pct': total_votes_pct[1],
            'vote3_pct': total_votes_pct[2],
            'vote4_pct': total_votes_pct[3],
            'vote5_pct': total_votes_pct[4],
            'vote6_pct': total_votes_pct[5]
        }

    # Append total votes data to the main data list
    data.extend(total_votes_data.values())

    df = pd.DataFrame(data)
    df.to_csv(f'{local_disk}/results_quickcount.csv', index=False)

    # Update Bubble datamart
    res = requests.get(f'{url_bubble}/AggregateRegion', headers=headers)

    if res.json()['response']['count'] == 0:
        # Perform bulk insert if the table is empty
        data = '\n'.join([
            json.dumps({
                "Event ID": row["event_id"],
                "Region": row["region"],
                "Paslon 1": row["vote1_pct"],
                "Paslon 2": row["vote2_pct"],
                "Paslon 3": row["vote3_pct"],
                "Paslon 4": row["vote4_pct"],
                "Paslon 5": row["vote5_pct"],
                "Paslon 6": row["vote6_pct"]
            })
            for _, row in df.iterrows()
        ])
        out = requests.post(f'{url_bubble}/AggregateRegion/bulk', headers=headers_bulk, data=data)

    else:
        # Update existing records based on their IDs
        existing_records = res.json()['response']['results']
        existing_ids = {record['Region']: record['_id'] for record in existing_records}

        for _, row in df.iterrows():
            payload = {
                'Event ID': row['event_id'],
                'Region': row['region'],
                'Paslon 1': row['vote1_pct'],
                'Paslon 2': row['vote2_pct'],
                'Paslon 3': row['vote3_pct'],
                'Paslon 4': row['vote4_pct'],
                'Paslon 5': row['vote5_pct'],
                'Paslon 6': row['vote6_pct']
            }
            region = row['region']
            if region in existing_ids:
                record_id = existing_ids[region]
                requests.patch(f'{url_bubble}/AggregateRegion/{record_id}', headers=headers, data=payload)
            else:
                requests.post(f'{url_bubble}/AggregateRegion', headers=headers, json=payload)