import sys
import os

from erasor_msg_test.msg import node

import tf
import os
from geometry_msgs.msg import Pose
import cv2
import rospy
import rosbag
import progressbar

from sensor_msgs.msg import PointCloud2

BAG_PATH = "/home/ye/Downloads/corridor_50.bag"
OUT_PATH = "/home/ye/DORF_rep/dataset/half_gazebo/bag/corridor_50.bag"

# bar = progressbar.ProgressBar()

"""
IN BAG
types:       sensor_msgs/PointCloud2 [1158d486dd51d683ce2f1be655c3c181]
             tf2_msgs/TFMessage      [94810edda583a504dfda3829e70d7eec]
topics:      /os_cloud_node/points     2190 msgs    : sensor_msgs/PointCloud2
             /tf                     229978 msgs    : tf2_msgs/TFMessage     
             /tf_static                   1 msg     : tf2_msgs/TFMessage
"""

"""
OUT BAG
types:       remo/node [369562eda9795c55aa23013a6af260da]
topics:      /node/combined/optimized   2190 msgs    : erasor/node
"""
# Open input and output bags

odom_data = {}

with rosbag.Bag(BAG_PATH, 'r') as inbag, rosbag.Bag(OUT_PATH, 'w') as outbag:
    
    # First pass: Collect all odom data
    for topic, msg, t in inbag.read_messages():
        if topic == "/tf" or topic == "/odom":  # Adjust this based on the exact odom topic
            pose_msg = Pose()
            
        if hasattr(msg, 'transforms'):
            transform = msg.transforms[0]
            pose_msg.position.x = transform.transform.translation.x
            pose_msg.position.y = transform.transform.translation.y
            pose_msg.position.z = transform.transform.translation.z
            pose_msg.orientation = transform.transform.rotation
            
            # Store the Pose with the timestamp as key
            odom_data[transform.header.stamp.to_sec()] = pose_msg

    # Second pass: Process PointCloud2 messages and match with odom data
    for topic, msg, t in inbag.read_messages():
        
        print("topic: ", topic)
        # Only process PointCloud2 messages from /os_cloud_node/points
        if topic == "/os_cloud_node/points":
            out = node()
            
            # Assign the PointCloud2 message directly to out.lidar
            out.lidar = msg
            
            # Find the closest odom message based on timestamp
            pointcloud_time = msg.header.stamp.to_sec()
            closest_time = min(odom_data.keys(), key=lambda k: abs(k - pointcloud_time))
            out.odom = odom_data[closest_time]  # Use the matched odom data
            
            # Use the header from the PointCloud2 message for synchronization
            out.header = msg.header
            
            # Write the combined node message to the output bag
            outbag.write('/node/combined/optimized', out, out.header.stamp)
            
            print("Processed message at time {}".format(out.header.stamp))