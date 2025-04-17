

# This one seems to be working correctly for now.




import os
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

    # Set the absolute base directory where your files are located.
    base_dir = "/home/kavach_d/Project/A-llmRec-my-code/data/amazon"

    # Build the absolute file path for the review data file.
    file_path = os.path.join(base_dir, f'{fname}.json.gz')

    # First pass: counting interactions for each user and item.
    for l in parse(file_path):
        line += 1
        asin = l['asin']
        rev = l['reviewerID']
        time = l['unixReviewTime']
        countU[rev] += 1
        countP[asin] += 1

    usermap = {}
    usernum = 0
    itemmap = {}  # maps ASIN to an initial (possibly non-sequential) ID
    itemnum = 0
    User = {}
    review_dict = {}
    name_dict = {'title': {}, 'description': {}}

    # Build the absolute path for the meta file.
    meta_file_path = os.path.join(base_dir, f'meta_{fname}.json')
    with open(meta_file_path, 'r') as f:
        json_data = f.readlines()
    data_list = [json.loads(line.strip()) for line in json_data]
    meta_dict = {}
    for entry in data_list:
        meta_dict[entry['asin']] = entry

    print(f"Loaded meta_dict with {len(meta_dict)} entries for dataset {fname}")

    # Create sets to track unique ASINs before and after filtering for valid meta data.
    all_valid_asins = set()
    filtered_valid_asins = set()

    # Second pass: process the review data and build mappings.
    for l in parse(file_path):
        asin = l['asin']
        rev = l['reviewerID']
        time = l['unixReviewTime']

        # Set threshold for filtering interactions.
        threshold = 5
        if ('Beauty' in fname) or ('Toys' in fname) or ('Magazine_Subscriptions' in fname):
            threshold = 4

        if countU[rev] < threshold or countP[asin] < threshold:
            continue

        # Add the ASIN to the set of items passing the threshold.
        all_valid_asins.add(asin)

        # Option B: Filter out reviews with missing meta data.
        if asin not in meta_dict:
            print(f"Warning: Skipping review for ASIN {asin} due to missing meta data.")
            continue

        # Only if meta data exists, add the ASIN to the filtered set.
        filtered_valid_asins.add(asin)

        # Map reviewer to a new integer ID.
        if rev in usermap:
            userid = usermap[rev]
        else:
            usernum += 1
            userid = usernum
            usermap[rev] = userid
            User[userid] = []

        # Map product ASIN to an initial item ID.
        if asin in itemmap:
            itemid = itemmap[asin]
        else:
            itemnum += 1
            itemid = itemnum
            itemmap[asin] = itemid
        User[userid].append([time, itemid])

        # Build review_dict (optional) and name_dict with meta data.
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

        # Since we know the meta data exists, add it directly.
        if len(meta_dict[asin].get('description', '')) == 0:
            name_dict['description'][itemmap[asin]] = 'Empty description'
        else:
            # Assuming description is a list; use the first element.
            name_dict['description'][itemmap[asin]] = meta_dict[asin]['description'][0]
        name_dict['title'][itemmap[asin]] = meta_dict[asin]['title']

    # Print counts before and after filtering.
    print(f"Total unique items that pass threshold (before meta check): {len(all_valid_asins)}")
    print(f"Unique items with valid meta data (after filtering): {len(filtered_valid_asins)}")
    missing_count = len(all_valid_asins) - len(filtered_valid_asins)
    print(f"Unique items missing meta data: {missing_count}")
    print(f"Number of items with meta data before re-indexing: {len(name_dict['title'])}")

    # --- Re-indexing step ---
    new_title = {}
    new_description = {}
    new_itemmap = {}  # maps old item id to new sequential id
    new_index = 1
    # Sort keys for reproducibility.
    for old_key in sorted(name_dict['title'].keys()):
        new_itemmap[old_key] = new_index
        new_title[new_index] = name_dict['title'][old_key]
        new_description[new_index] = name_dict['description'][old_key]
        new_index += 1
    # Replace name_dict with re-indexed dictionaries.
    name_dict['title'] = new_title
    name_dict['description'] = new_description
    new_itemnum = len(new_title)

    # Update the User interactions: remap item IDs using new_itemmap.
    for userid in User.keys():
        for i in range(len(User[userid])):
            old_itemid = User[userid][i][1]
            if old_itemid in new_itemmap:
                User[userid][i][1] = new_itemmap[old_itemid]
        User[userid].sort(key=lambda x: x[0])

    # Save the re-indexed meta data file.
    output_path = os.path.join(base_dir, f'{fname}_text_name_dict.json.gz')
    with open(output_path, 'wb') as tf:
        pickle.dump(name_dict, tf)

    # Write user-item interaction pairs using the new indexing.
    txt_output = os.path.join(base_dir, f'{fname}.txt')
    with open(txt_output, 'w') as f:
        for user in User.keys():
            for i in User[user]:
                f.write('%d %d\n' % (user, i[1]))

    print("User count:", usernum, "Original item count:", itemnum, "Re-indexed item count:", new_itemnum)

