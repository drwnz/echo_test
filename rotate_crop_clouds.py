#!/usr/bin/env python
#
#  Copyright 2021 [David Robert Wong]
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  ********************
#  v0.1.0: drwnz (david.wong@tier4.jp)
#
#  roate_crop_clouds.py
#
#  Created on: February 13th 2021
#

import rosbag
import sys
import rospy
from sensor_msgs.msg import PointCloud2, PointField
import sensor_msgs.point_cloud2 as point_cloud2
import tf2_ros
import numpy as np
from tf2_sensor_msgs.tf2_sensor_msgs import do_transform_cloud
from geometry_msgs.msg import TransformStamped
from std_msgs.msg import Header

# The rosbag to process
in_bagfile = str(sys.argv[1])
out_bagfile = str(sys.argv[2])
start_time = float(sys.argv[3])
end_time = float(sys.argv[4])

pointcloud_topic = "/velodyne_points_ex"

def crop_topic_data(point_cloud_list, crop_list):

    # Crop parameters
    x_min = crop_list[0][0]
    x_max = crop_list[0][1]

    y_min = crop_list[1][0]
    y_max = crop_list[1][1]

    z_min = crop_list[2][0]
    z_max = crop_list[2][1]

    # Cropping rows with out-of-range values
    cropped_cloud = np.array(point_cloud_list)
    cropped_cloud = cropped_cloud[(cropped_cloud[:,0] >= x_min) & (cropped_cloud[:,0] <= x_max)]
    cropped_cloud = cropped_cloud[(cropped_cloud[:,1] >= y_min) & (cropped_cloud[:,1] <= y_max)]
    cropped_cloud = cropped_cloud[(cropped_cloud[:,2] >= z_min) & (cropped_cloud[:,2] <= z_max)]
    return cropped_cloud.tolist()

def transform_pointcloud(pointcloud):
    trans = TransformStamped()
    trans.header.stamp = pointcloud.header.stamp
    trans.header.frame_id = pointcloud.header.frame_id
    trans.header.seq = pointcloud.header.seq
    trans.child_frame_id = "rotated"
    trans.transform.translation.x = 0
    trans.transform.translation.y = 0
    trans.transform.translation.z = 0
    trans.transform.rotation.x = 0
    trans.transform.rotation.y = 0
    trans.transform.rotation.z = -0.022
    trans.transform.rotation.w = 1.0

    newpointcloud = do_transform_cloud(pointcloud, trans)
    return newpointcloud

crop_list = [(-23.8, 11.5), (-46.0, 16.8), (-2.01, 8.0)]
with rosbag.Bag(out_bagfile, 'w') as out_bag:
    for topic, msg, t in rosbag.Bag(in_bagfile).read_messages():
        if topic == pointcloud_topic and msg.header.stamp.to_sec() > start_time and msg.header.stamp.to_sec() < end_time:
            cropped_points = crop_topic_data(list(point_cloud2.read_points(transform_pointcloud(msg), skip_nans=True, field_names = ("x", "y", "z", "intensity", "return_type"))), crop_list)

            for i in range(len(cropped_points)):
                cropped_points[i][-1] = int(cropped_points[i][-1])
                cropped_points[i][-2] = int(cropped_points[i][-2])

            fields = [PointField('x', 0, PointField.FLOAT32, 1),
                      PointField('y', 4, PointField.FLOAT32, 1),
                      PointField('z', 8, PointField.FLOAT32, 1),
                      PointField('intensity', 12, PointField.UINT32, 1),
                      PointField('return_type', 16, PointField.UINT8, 1)
                      ]

            header = Header()
            header = msg.header
            write_cloud = point_cloud2.create_cloud(header, fields, cropped_points)
            out_bag.write(topic, write_cloud, msg.header.stamp)
