#!/usr/bin/env python3.6

import argparse
import collections
import os
import queue
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor

import aiofiles
import boto3

extra_params = {'s3': {'addressing_style': 'path'}, 'signature_version': 's3v4'}

MB1 = 1024 * 1024


class S3MultipartUpload(object):
    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM = int(5e6)

    def __init__(self,
                 endpoint_url,
                 bucket,
                 key,
                 local_path,
                 port=9090,
                 part_size=int(15e6),
                 region_name="eu-west-1",
                 verbose=False):
        self.endpoint_url = endpoint_url
        self.port = port
        self.bucket = bucket
        self.key = key
        self.path = local_path
        self.total_bytes = os.stat(local_path).st_size
        self.part_bytes = part_size
        self.s3 = boto3.session.Session(region_name=region_name). \
            client("s3", use_ssl=False, endpoint_url=":".join([self.endpoint_url, str(self.port)]),
                   aws_access_key_id='aaa',
                   aws_secret_access_key='bbb',
                   config=boto3.session.Config(
                       extra_params),
                   region_name='us-east-1')
        if verbose:
            boto3.set_stream_logger(name="botocore")

    async def abort_all(self):
        mpus = self.s3.list_multipart_uploads(Bucket=self.bucket)
        aborted = []
        print("Aborting", len(mpus), "uploads")
        await asyncio.sleep(1)
        if "Uploads" in mpus:
            for u in mpus["Uploads"]:
                upload_id = u["UploadId"]
                aborted.append(
                    self.s3.abort_multipart_upload(
                        Bucket=self.bucket, Key=self.key, UploadId=upload_id))
        return aborted

    def create(self):
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        mpu_id = mpu["UploadId"]
        return mpu_id

    def upload(self, mpu_id):
        parts_to_complete_queue = queue.Queue()
        self._upload(mpu_id, self.path, parts_to_complete_queue)
        return list(parts_to_complete_queue.queue)

    def _upload(self, mpu_id, file_path, parts_to_complete_queue):
        incoming_parts_queue = queue.Queue()
        parts_num = os.stat(file_path).st_size // self.part_bytes
        if os.stat(file_path).st_size % self.part_bytes:  # in case there are leftovers
            parts_num += 1
        print("Parts Number: {}".format(parts_num))
        #  Preparing queue containing (part_num, offset)
        [incoming_parts_queue.put((i + 1, i * self.part_bytes)) for i in range(parts_num)]
        with ThreadPoolExecutor() as executor:
            for i in range(100):
                executor.submit(self._mp_upload_worker, mpu_id, file_path, incoming_parts_queue,
                                parts_to_complete_queue)

    async def mp_upload_async(self, mpu_id, file_path, incoming_parts_queue, parts_to_complete_queue):
        try:
            while True:
                part_num, offset = incoming_parts_queue.get_nowait()
                async with aiofiles.open(file_path, 'rb') as fp:
                    await fp.seek(offset)
                    data = await fp.read(self.part_bytes)
                    part = self.s3.upload_part(
                        Body=data, Bucket=self.bucket, Key=self.key, UploadId=mpu_id, PartNumber=part_num)
                parts_to_complete_queue.put({"PartNumber": part_num, "ETag": part["ETag"]})
                print("Part: {} Data Length: {}".format(part_num, len(data)))
        except queue.Empty:
            pass

    # async def _uploader(self, mpu_id, part_num, file_path, offset):
    #     with open(file_path, 'rb') as fp:
    #         fp.seek(offset)
    #         data = fp.read(self.part_bytes)
    #         part = self.s3.upload_part(
    #             Body=data, Bucket=self.bucket, Key=self.key, UploadId=mpu_id, PartNumber=part_num)
    #         return part

    async def complete(self, mpu_id, parts_to_complete_queue):
        parts = list(parts_to_complete_queue.queue)
        result = self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts})
        return result


# Helper
def as_percent(num, denom):
    return float(num) / float(denom) * 100.0


def parse_args():
    parser = argparse.ArgumentParser(description='Multipart upload')
    parser.add_argument('endpoint_url', default=None)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--path', required=True)
    parser.add_argument('--port', default=9090, type=int)
    parser.add_argument('--part_size', default=S3MultipartUpload.PART_MINIMUM, type=int)
    parser.add_argument('--region', default="eu-west-1")
    return parser.parse_args()


def main():
    args = parse_args()
    mpu = S3MultipartUpload(
        args.endpoint_url,
        args.bucket,
        args.key,
        args.path,
        port=args.port,
        part_size=args.part_size,
        region_name=args.region)


    # abort all multipart uploads for this bucket (optional, for starting over)
    mpu.abort_all()
    # create new multipart upload
    mpu_id = mpu.create()
    # upload parts
    #parts = mpu.upload(mpu_id)
    incoming_parts_queue = queue.Queue()
    parts_to_complete_queue = queue.Queue()
    parts_num = os.stat(mpu.path).st_size // mpu.part_bytes
    if os.stat(mpu.path).st_size % mpu.part_bytes:  # in case there are leftovers
        parts_num += 1
    [incoming_parts_queue.put((i + 1, i * mpu.part_bytes)) for i in range(parts_num)]
    print("Parts Number: {}".format(parts_num))
    tasks = [
        asyncio.ensure_future(mpu.mp_upload_async(mpu_id, mpu.path, incoming_parts_queue, parts_to_complete_queue)) for _ in range(32)
    ] + [
        asyncio.ensure_future(mpu.complete(mpu_id, parts_to_complete_queue)),
        asyncio.ensure_future(mpu.abort_all())
    ]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    parts = list(parts_to_complete_queue.queue)
    print("Parts: {}".format(parts))
    # complete multipart upload
    # print(mpu.complete(mpu_id, parts_to_complete_queue))


if __name__ == "__main__":
    main()
