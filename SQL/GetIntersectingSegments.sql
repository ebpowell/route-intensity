
 SELECT clipped.objectid, clipped.gid, clipped.district_na, clipped.clipped_geom, azimuth(st_startpoint(clipped.clipped_geom), st_endpoint(clipped.clipped_geom)) into clippedarcs2
   FROM ( SELECT nextval('lpseq'::regclass) as objectid, zipclip.gid, zipclip.district_na, 
   (st_dump(st_intersection(zipclip.the_geom, countylines.the_geom))).geom AS clipped_geom
           FROM zipclip
      JOIN countylines ON st_intersects(zipclip.the_geom, countylines.the_geom)) clipped
  WHERE st_dimension(clipped.clipped_geom) = 1;



