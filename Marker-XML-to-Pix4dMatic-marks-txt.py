from lxml import etree
import pandas as pd
import os
from os.path import join
from pandasql import sqldf

# Initialize SQL query function
mysql = lambda q: sqldf(q, globals())

# Configuration settings
network = 'config1'
Files_Path = {
    'out_folder': r'OutPut\\',
    'Metashape_gcp': r'input_data\Mavic3E_GCP_Initial.xml',
}


def cameraIDfinder(Files_Path):
    """Extract camera information from XML file"""
    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(Files_Path['Metashape_gcp'], parser)
    root = tree.getroot()
    return pd.DataFrame([
        {'id': cam.get('id'), 'label': cam.get('label')}
        for cam in root.xpath('//camera')
    ])


def GCPfinder(Files_Path, camera_lookup):
    """Extract GCP points and markers from XML file"""
    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(Files_Path['Metashape_gcp'], parser)
    root = tree.getroot()
    
    points_info = []
    marker_info = []
    
    for point in root.xpath('//marker'):
        point_id = point.get('id') or point.get('marker_id')
        
        if point.get('id') is not None:  # It's a control point
            reference = point.find('reference')
            points_info.append({
                'ID': point_id,
                'Type': 'ControlPoint',
                'Name': point.get('label'),
                'Category': 'Full',
                'X': reference.get('x'),
                'Y': reference.get('y'),
                'Z': reference.get('z'),
                'HorizontalAccuracy': reference.get('sxy'),
                'VerticalAccuracy': reference.get('sz'),
                'CheckPoint': reference.get('enabled')
            })
        else:  # It's a marker with measurements
            measurements = []
            markerid = point.get('marker_id') 
            for loc in point.xpath('./location'):
                photo_id = loc.get('camera_id')

                image_name = None
                camera_label = next(
                    (label for label, cam_id in camera_lookup.items() if cam_id == photo_id),
                    None
                )

                measurement = {
                        'PhotoId': camera_label+'.JPG',
                        'x_px': loc.get('x'),  # Pixel coordinates
                        'y_px': loc.get('y'),
                    }
                    
                measurements.append(measurement)
            
            marker_info.append({
                'MarkerID': markerid,
                'Type': 'Marker',
                'Measurement': measurements
            })
    
    return points_info, marker_info

def merge_gcp_data(control_points, markers):
    merged_data = []
    cp_dict = {cp['ID']: cp for cp in control_points}
    for marker in markers:
        marker_id = marker['MarkerID']
        
        if marker_id in cp_dict:
            merged_item = {
                **cp_dict[marker_id], 
                **marker,              
                'Type': 'Integrated'   
            }
            merged_data.append(merged_item)
            
          
            del cp_dict[marker_id]
        else:
    
            merged_data.append({
                **marker,
                'Type': 'MarkerOnly'
            })
    
    for cp_id, cp in cp_dict.items():
        merged_data.append({
            **cp,
            'Type': 'ControlPointOnly'
        })
    
    return merged_data

def Marks_creator(control_points):
    marks=[]
    for point in control_points:
        for m in point['Measurement']:
            marks.append({'im':m['PhotoId'],'gcp':point['Name'],'px':m['x_px'],'py':m['y_px'],'t':10})
            
    return pd.DataFrame(marks)  



if __name__ == '__main__':
    # Load and process data
    camera_info = cameraIDfinder(Files_Path)
    camera_lookup = {row['label']: row['id'] for _, row in camera_info.iterrows()}
    points_info, marker_info = GCPfinder(Files_Path, camera_lookup)
    
    # Merge and save results
    control_points = merge_gcp_data(points_info, marker_info)
    marks=Marks_creator(control_points)
    # Save other outputs if needed
    marks.to_csv(join(Files_Path['out_folder'], 'Maks_pix4d_Mavic3E.txt'), index=False)