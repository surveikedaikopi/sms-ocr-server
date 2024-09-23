import json
import requests
import pandas as pd

from config.config import *


# Functions to fetch and save quick count results
def fetch_quickcount():
    try:
        res = requests.get(url_get_event_ids, headers=headers)
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch event IDs: {e}")
        return

    out = res.json()['response']
    
    list_event_id = out['list_events']
    list_event_type = out['list_types']
    list_event_name = out['list_names']

    data = []
    total_votes_data = {}

    for event_id, event_type, event_name in zip(list_event_id, list_event_type, list_event_name):
        params = {'Event ID': event_id}
        if event_type == 'Pilpres':
            url_votes = url_votes_agg_pilpres
        elif event_type == 'Pilgub':
            url_votes = url_votes_agg_provinsi
        elif event_type in ['Pilwalkot', 'Pilbup']:
            url_votes = url_votes_agg_kabkota

        try:
            res = requests.get(url_votes, headers=headers, params=params)
            res.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch votes for event {event_id}: {e}")
            continue

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
                'event_name': event_name,
                'event_type': event_type,
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
            'event_name': event_name,
            'event_type': event_type,
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
    try:
        res = requests.get(f'{url_bubble}/AggregateRegion', headers=headers)
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to update Bubble datamart: {e}")
        return

    # Fetch all event IDs first
    try:
        res = requests.get(url_get_event_ids, headers=headers)
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch event IDs: {e}")
        return

    event_ids = res.json()['response']['list_events']

    # Initialize an empty list to store all existing records
    existing_records = []

    # Loop over each event_id and fetch records
    for event_id in event_ids:
        filter_params = [{"key": "Event ID", "constraint_type": "equals", "value": event_id}]
        try:
            res = requests.get(f'{url_bubble}/AggregateRegion', headers=headers, params={"constraints": json.dumps(filter_params)})
            res.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch records for event ID {event_id}: {e}")
            continue

        response_json = res.json()
        records = response_json['response']['results']
        existing_records.extend(records)

    # Debug: Print the total number of records in the response
    # print(f"Total records in response: {len(existing_records)}")

    # Create the existing_ids dictionary
    existing_ids = {(record['Event ID'].strip().lower(), record['Region'].strip().lower()): record['_id'] for record in existing_records}

    # Debug: Print the total number of existing records
    # print(f"Total existing records: {len(existing_ids)}")

    # Debug: Print the existing IDs
    # print(existing_ids)

    if len(existing_records) == 0:
        # Perform bulk insert if the table is empty
        data = '\n'.join([
            json.dumps({
                "Event ID": row["event_id"],
                "Event Name": row["event_name"],  # Added event_name
                "Event Type": row["event_type"],  # Added event_type
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
        try:
            out = requests.post(f'{url_bubble}/AggregateRegion/bulk', headers=headers_bulk, data=data)
            out.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to perform bulk insert: {e}")
            return

    else:
        # Update existing records based on their IDs
        for _, row in df.iterrows():
            payload = {
                'Event ID': row['event_id'],
                'Event Name': row['event_name'],  # Added event_name
                'Event Type': row['event_type'],  # Added event_type
                'Region': row['region'],
                'Paslon 1': row['vote1_pct'],
                'Paslon 2': row['vote2_pct'],
                'Paslon 3': row['vote3_pct'],
                'Paslon 4': row['vote4_pct'],
                'Paslon 5': row['vote5_pct'],
                'Paslon 6': row['vote6_pct']
            }
            key = (row['event_id'].strip().lower(), row['region'].strip().lower())
            if key in existing_ids:
                record_id = existing_ids[key]
                # print(f"Updating record: key={key}, existing_id={record_id}")  # Debug print
                try:
                    requests.patch(f'{url_bubble}/AggregateRegion/{record_id}', headers=headers, data=payload)
                except requests.exceptions.RequestException as e:
                    print(f"Failed to update record {record_id}: {e}")
            else:
                # print(f"Inserting new record: key={key}")  # Debug print
                try:
                    requests.post(f'{url_bubble}/AggregateRegion', headers=headers, json=payload)
                except requests.exceptions.RequestException as e:
                    print(f"Failed to insert new record for key {key}: {e}")