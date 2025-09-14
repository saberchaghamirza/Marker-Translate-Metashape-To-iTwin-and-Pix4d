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
    'input_image_path': r'G:\ghazvin all data\03 ImagingAreaA_125h\14040320_UOT_Mavic3E_Tilt21-GSD2cm_8580_WingterraSimulation',
    'out_folder': r'OutPut\\',
    'Metashape_gcp': r'input_data\gcp_initilal_matrix300_gsd1.5_tilt35.xml',
}

def get_image_paths(root_folder, extensions=('.jpg', '.jpeg', '.png', '.tif')):
    """Recursively get all image paths with specified extensions"""
    return [
        {'name': os.path.splitext(f)[0], 'path': join(root, f)}
        for root, _, files in os.walk(root_folder)
        for f in files
        if f.lower().endswith(extensions)
    ]

def cameraIDfinder(Files_Path):
    """Extract camera information from XML file"""
    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(Files_Path['Metashape_gcp'], parser)
    root = tree.getroot()
    return pd.DataFrame([
        {'id': cam.get('id'), 'label': cam.get('label')}
        for cam in root.xpath('//camera')
    ])

def match_images_to_cameras(imagfiles, camera_info):
    """Match image files with camera IDs"""
    # Create lookup dictionaries
    camera_lookup = {row['label']: row['id'] for _, row in camera_info.iterrows()}
    image_paths = {img['name']: img['path'] for img in imagfiles}
    
    cameras = []
    matched_photos = set()
    
    # Find matching photo IDs
    for img in imagfiles:
        photo_id = next(
            (cam_id for label, cam_id in camera_lookup.items() if img['name'] in label),
            None
        )
        if photo_id:
            cameras.append({'PhotoId': photo_id, 'ImagePath': img['path']})
            matched_photos.add(photo_id)
    
    return cameras, image_paths, matched_photos, camera_lookup

def GCPfinder(Files_Path, image_paths, matched_photos, camera_lookup):
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
                measurement = {
                    'PhotoId': photo_id,
                    'x_px': loc.get('x'),  # Pixel coordinates
                    'y_px': loc.get('y'),
                }
                
                # Add image path if photo is matched
                if photo_id in matched_photos:
                    camera_label = next(
                        (label for label, cam_id in camera_lookup.items() if cam_id == photo_id),
                        None
                    )
                    if camera_label and camera_label in image_paths:
                        measurement['ImagePath'] = image_paths[camera_label]
                
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


from lxml.builder import E

def create_survey_xml(control_points, Files_Path):
    surveys_data = E.SurveysData(
        E.SpatialReferenceSystems(
            E.SRS(
                E.Id("2"),
                E.Name("WGS 84 / UTM zone 39N (EPSG:32639)"),
                E.Definition("EPSG:32639")
            )
        ),
        E.ControlPoints()
    )

   
    for cp in control_points:
        control_point = E.ControlPoint(
            E.Id(str(cp['ID'])),
            E.SRSId("2"),
            E.Name(cp['Name']),
            E.Category("Full"),
            E.Position(
                E.x(str(cp['X'])),
                E.y(str(cp['Y'])),
                E.z(str(cp['Z']))
            ),
            E.HorizontalAccuracy(str(cp['HorizontalAccuracy'])),
            E.VerticalAccuracy(str(cp['VerticalAccuracy'])),
            E.CheckPoint(str(cp['CheckPoint']).lower())
        )

      
        if 'Measurement' in cp:
            for meas in cp['Measurement']:
                measurement = E.Measurement(
                    E.PhotoId(str(meas['PhotoId'])),
                    E.ImagePath(meas['ImagePath']),
                    E.x(str(meas['x_px'])),
                    E.y(str(meas['y_px']))
                )
                control_point.append(measurement)

        surveys_data.find('ControlPoints').append(control_point)
    output_path =  Files_Path['out_folder']+'itwin_markers.xml'
    tree = etree.ElementTree(surveys_data)
    tree.write(output_path, encoding='utf-8', xml_declaration=True, pretty_print=True)
# Main execution



if __name__ == '__main__':
    # Load and process data
    imagfiles = get_image_paths(Files_Path['input_image_path'])
    camera_info = cameraIDfinder(Files_Path)
    cameras, image_paths, matched_photos, camera_lookup = match_images_to_cameras(imagfiles, camera_info)
    points_info, marker_info = GCPfinder(Files_Path, image_paths, matched_photos, camera_lookup)
    
    # Merge and save results
    control_points = merge_gcp_data(points_info, marker_info)
    create_survey_xml(control_points,Files_Path)
    # merged_data.to_csv(join(Files_Path['out_folder'], 'merged_gcp_data.csv'), index=False)
    
    # Save other outputs if needed
    # pd.DataFrame(cameras).to_csv(join(Files_Path['out_folder'], 'matched_cameras.csv'), index=False)