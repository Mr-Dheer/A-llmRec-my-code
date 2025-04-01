import os
import os.path
import gzip
import json
import pickle
from tqdm import tqdm
from collections import defaultdict


def parse(path):
    g = gzip.open(path, 'rb')
    for l in tqdm(g):
        yield json.loads(l)


def preprocess(fname):
    countU = defaultdict(lambda: 0)
    countP = defaultdict(lambda: 0)
    line = 0

    file_path = f'../../data/amazon/{fname}.json.gz'  # review data file

    # Counting interactions for each user and item
    for l in parse(file_path):
        line += 1
        asin = l['asin']
        rev = l['reviewerID']
        time = l['unixReviewTime']
        countU[rev] += 1
        countP[asin] += 1

    usermap = dict()
    usernum = 0
    itemmap = dict()  # maps ASIN to an initial (possibly non-sequential) ID
    itemnum = 0
    User = dict()
    review_dict = {}
    name_dict = {'title': {}, 'description': {}}

    # Load raw meta data
    with open(f'../../data/amazon/meta_{fname}.json', 'r') as f:
        json_data = f.readlines()
    data_list = [json.loads(line.strip()) for line in json_data]
    meta_dict = {}
    for l in data_list:
        meta_dict[l['asin']] = l

    # Process the review data
    for l in parse(file_path):
        line += 1
        asin = l['asin']
        rev = l['reviewerID']
        time = l['unixReviewTime']

        # Set threshold for filtering interactions
        threshold = 5
        if ('Beauty' in fname) or ('Toys' in fname) or ('Magazine_Subscriptions' in fname):
            threshold = 3

        if countU[rev] < threshold or countP[asin] < threshold:
            continue

        # Map reviewer to a new integer ID
        if rev in usermap:
            userid = usermap[rev]
        else:
            usernum += 1
            userid = usernum
            usermap[rev] = userid
            User[userid] = []

        # Map product ASIN to an initial item ID
        if asin in itemmap:
            itemid = itemmap[asin]
        else:
            itemnum += 1
            itemid = itemnum
            itemmap[asin] = itemid
        User[userid].append([time, itemid])

        # Build review_dict (optional) and name_dict with meta data
        if itemmap[asin] in review_dict:
            try:
                review_dict[itemmap[asin]]['review'][usermap[rev]] = l['reviewText']
            except Exception as e:
                pass
            try:
                review_dict[itemmap[asin]]['summary'][usermap[rev]] = l['summary']
            except Exception as e:
                pass
        else:
            review_dict[itemmap[asin]] = {'review': {}, 'summary': {}}
            try:
                review_dict[itemmap[asin]]['review'][usermap[rev]] = l['reviewText']
            except Exception as e:
                pass
            try:
                review_dict[itemmap[asin]]['summary'][usermap[rev]] = l['summary']
            except Exception as e:
                pass
        try:
            if len(meta_dict[asin]['description']) == 0:
                name_dict['description'][itemmap[asin]] = 'Empty description'
            else:
                name_dict['description'][itemmap[asin]] = meta_dict[asin]['description'][0]
            name_dict['title'][itemmap[asin]] = meta_dict[asin]['title']
        except Exception as e:
            pass

    # --- Re-indexing step ---
    # Create new dictionaries with sequential keys and a mapping from old to new IDs.
    new_title = {}
    new_description = {}
    new_itemmap = {}  # maps old item id to new sequential id
    new_index = 1
    # Sort keys for reproducibility; alternatively, you could iterate in any order.
    for old_key in sorted(name_dict['title'].keys()):
        new_itemmap[old_key] = new_index
        new_title[new_index] = name_dict['title'][old_key]
        new_description[new_index] = name_dict['description'][old_key]
        new_index += 1
    # Replace name_dict with re-indexed dictionaries
    name_dict['title'] = new_title
    name_dict['description'] = new_description
    # Update the total number of items to reflect the new indexing
    new_itemnum = len(new_title)

    # Update the User interactions: remap item IDs using new_itemmap.
    for userid in User.keys():
        for i in range(len(User[userid])):
            old_itemid = User[userid][i][1]
            if old_itemid in new_itemmap:
                User[userid][i][1] = new_itemmap[old_itemid]

    # Save the re-indexed meta data file
    with open(f'../../data/amazon/{fname}_text_name_dict.json.gz', 'wb') as tf:
        pickle.dump(name_dict, tf)

    for userid in User.keys():
        User[userid].sort(key=lambda x: x[0])

    print("User count:", usernum, "Original item count:", itemnum, "Re-indexed item count:", new_itemnum)

    # Write user-item interaction pairs using the new indexing
    with open(f'../../data/amazon/{fname}.txt', 'w') as f:
        for user in User.keys():
            for i in User[user]:
                f.write('%d %d\n' % (user, i[1]))
