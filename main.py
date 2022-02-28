from instagrapi import Client
import logging
import os
import requests
import schedule

# Login credentials to access private API
IG_USERNAME=os.environ.get("IGUSERNAME")
IG_PASSWORD=os.environ.get("IGPASSWORD")

# IG Account you'd like to scrape
IG_ACCOUNT=os.environ.get("IGACCOUNT")

# ID of Notion DB you'd like to upload data to
DB_ID = os.environ.get("NOTION_DB_ID")

# For logging purposes
run_number = 1


def get_media_type(media_product_type):
    """
    Determine what type of Instagram media we're dealing with.
    The type is enumerated as follows:
        Photo - When media_type=1
        Video - When media_type=2 and product_type=feed
        IGTV - When media_type=2 and product_type=igtv
        Reel - When media_type=2 and product_type=clips
        Album - When media_type=8
    """

    if media_product_type.startswith("1"): media_product_type = "1"
    if media_product_type.startswith("8"): media_product_type = "8"

    switcher = {
        "1": "Image",
        "2feed": "Video",
        "2igtv": "IGTV",
        "2clips": "Reel",
        "8": "Carousel",
    }

    try:
        media_type = switcher[media_product_type]
    except Exception:
        logging.warning(f'Run Number: {run_number} -- Media type not found. Check api and switcher function.')
        media_type = "Unknown"

    return media_type


def get_posts_user_tagged_in(cl):
    """
    For the given user, this function gets all posts the user is tagged in
    Creates a custom media object that contains data to be stored in Notion
    """
    tagged_posts = cl.usertag_medias(cl.user_id_from_username(IG_ACCOUNT), 20)

    # Media Object: [{media_pk, tagger_username, media_type, image_url, video_url, absolute_url}].
    try:
        tagged_posts = list(map(
            lambda x: {
                "media_pk": x.pk,
                "tagger_username": x.user.username,
                "media_type": get_media_type(str(x.media_type) + x.product_type),
                "image_url": cl.media_info(x.pk).resources[0].thumbnail_url if get_media_type(str(x.media_type) + x.product_type) == "Carousel" else x.thumbnail_url,
                "video_url": "" if get_media_type(str(x.media_type) + x.product_type) in ["Photo", "Carousel"] else x.video_url,
                "absolute_url": f"https://www.instagram.com/{'tv' if get_media_type(str(x.media_type) + x.product_type) == 'IGTV' else 'p'}/{x.code}/",
                "user_profile_url": f"https://www.instagram.com/{x.user.username}/ " 
            }, 
            tagged_posts))
    except Exception:
        logging.error(f"Run Number: {run_number} -- Can't construct one of the Media Object, review what the 'usertag_medias' method is returning as the structure may have changed.")
        tagged_posts = []

    logging.info(f"Run Number: {run_number} -- The posts the user was tagged in: {tagged_posts}")

    return tagged_posts


def check_for_duplicate_image_in_db(db_id, media_pk):
    """
    Check to make sure that the image we are about to upload to Notion isn't already in the DB
    """

    url = f"https://api.notion.com/v1/databases/{db_id}/query"

    headers = {
        "Accept": "application/json",
        "Notion-Version": "2021-08-16",
        "Content-Type": "application/json",
        "Authorization": f"Bearer secret_K7Oq4q16Ez5xAnXI2GzIDBw1xkr5JCrlFZorPZrJ9A6"
    }

    payload = {
        "page_size": 5,
        "filter": {
            "property": "Media ID",
            "text": {
                "equals": f"{media_pk}"
            }
        }
    }

    response = requests.post(
        url,
        json=payload,
        headers=headers,
    )

    try:
        num_of_matching_pages = len(response.json()["results"])
    except Exception:
        logging.error(f"Run Number: {run_number} -- An error determining if the media_item {media_pk} exists in database already. Probably a change in schema. Check the Notion API response for a filtered DB query. Or a request error.")
        logging.error( f"Run Number: {run_number} -- {response.text}")
        num_of_matching_pages = 1

    logging.info(f"Run Number: {run_number} -- {f'Media item {media_pk} exists' if num_of_matching_pages >= 1 else f'Media item {media_pk} does not exist'}")

    return num_of_matching_pages >= 1


def create_page_in_db(payload):
    """
    Creates the page in the Notion DB populating it with the content and properties retreieved from IG
    """
    url = "https://api.notion.com/v1/pages"

    headers = {
        "Notion-Version": "2021-08-16",
        "Content-Type": "application/json",
        "Authorization": f"Bearer secret_K7Oq4q16Ez5xAnXI2GzIDBw1xkr5JCrlFZorPZrJ9A6"
    }

    response = requests.post(
        url,
        json=payload,
        headers=headers,
    )
    
    try:
        added_media_pk = response.json()["properties"]["Media ID"]["rich_text"][0]["text"]["content"]
        logging.info(f"Run Number: {run_number} -- Successfully uploaded {added_media_pk} to Notion.")
    except Exception:
        logging.error(f"Run Number: {run_number} -- Couldn't add {payload['properties']['Media ID']['rich_text'][0]['text']['content']} to Notion DB. Likely a validation or request error.")
        logging.error(f"Run Number: {run_number} -- {response.text}")
        added_media_pk = None

    return added_media_pk


def construct_payload(media_object, isRetry=False):
    """
    Constructs the payload to be sent in the POST request
    To re-use double check the property IDs, will need to
    match IDs in existing DB
    """
    # property ids will have to be retrieved first and then inserted in
    is_video = media_object["media_type"] not in ["Image", "Carousel"]

    if not isRetry:
        children = [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {
                        "url": media_object["image_url"]
                    }
                }
            },
        ]

        if is_video and media_object["video_url"]: children.append({
                "object": "block",
                "type": "video",
                "video": {
                    "type": "external",
                    "external": {
                        "url": media_object["video_url"]
                    }
                }
            })
    else:
        children = [{
            "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "text": [{
                        "type": "text",
                        "text": {
                            "content": "Images unavailable. Download Manually.",
                            "link": {
                                "type": "url",
                                "url": media_object["absolute_url"]
                            }
                        }
                    }]
                }
        }]

    return {
        "parent": {"type": "database_id", "database_id": DB_ID},
        "properties": {
            "Username": {
                "id": r"Ao%3BW",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": media_object["tagger_username"],
                            "link": {
                                "type": "url",
                                "url": media_object["user_profile_url"]
                            }
                        }
                    }
                ]
            },
            "Platform": {
                "id": r"jEBI",
                "type": "select",
                "select": {
                    "name": "Instagram",
                }
            },
            "Type": {
                "id": "Oxto",
                "type": "select",
                "select": { 
                    "name": "Image" if media_object["media_type"] == "Carousel" else media_object["media_type"],
                }
            },
            "Media ID": {
                "id": r"vZ%7B%3C",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": media_object["media_pk"]
                        }
                    }
                ]
            },
            "Link": {
                'id': r"%5E%7CWk",
                'type': 'url',
                'url': media_object["absolute_url"]
            }
        },
        "children": children
    }


def add_media_to_notion(media_object):
    """
    Main 
    """
    if check_for_duplicate_image_in_db(DB_ID, media_object["media_pk"]):
        return
    payload = construct_payload(media_object)
    added_media_pk = create_page_in_db(payload)
    if added_media_pk is not None:
        return added_media_pk
    else:
        """We retry to handle any errors in the children blocks. We still create the database item but with
        a fallback error message in place of the images."""
        payload = construct_payload(media_object, True)
        logging.info(f"Run Number: {run_number} -- Retrying to upload {media_object['media_pk']}")
        return create_page_in_db(payload)


def main():
    logging.info(f"Run Number: {run_number} -- Let's get started!")

    cl = Client()

    # To avoid security flags we always use the same settings so we always login from one device and one IP 
    if os.path.exists("dump.json"):
        cl.load_settings("dump.json")
    else:
        with open("dump.json", 'x') as f:
            pass

    cl.login(IG_USERNAME, IG_PASSWORD)

    cl.dump_settings('dump.json')

    tagged_posts = get_posts_user_tagged_in(cl)

    logging.info(f"Run Number: {run_number} -- Number of tagged posts: {len(tagged_posts)}")

    media_pks_of_items_added_to_notion = []
    for media_object in tagged_posts:
        added_media_pk = add_media_to_notion(media_object)
        if added_media_pk is not None: media_pks_of_items_added_to_notion.append(added_media_pk)

    logging.info(f"Run Number: {run_number} -- Media Items added to Notion: {media_pks_of_items_added_to_notion}")
    logging.info(f"Run Number: {run_number} -- A total of {len(media_pks_of_items_added_to_notion)} media items were added to Notion")
    media_pks_of_items_added_to_notion.clear()
    logging.info(f"Run Number: {run_number} -- Finished running, goodbye! :)")


if __name__ == "__main__":
    logging.basicConfig(
        filename='application.log',
        encoding='utf-8',
        level=logging.INFO,
        format='%(asctime)s %(message)s',
        datefmt='%d/%m/%Y %I:%M:%S %p'
    )

    main()

    schedule.every(5).minutes.do(main)

    while True:
        run_number = run_number + 1
        schedule.run_pending()
