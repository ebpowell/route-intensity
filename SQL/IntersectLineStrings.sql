

SELECT clipped.gid, clipped.district_na, st_astext(clipped_geom)
FROM (SELECT nextval('lpseq'::regclass) as gid, zipclip.district_na, 
(ST_Dump(ST_Intersection(zipclip.the_geom, countylines.the_geom))).geom As clipped_geom
FROM zipclip
	INNER JOIN countylines
	ON ST_Intersects(zipclip.the_geom, countylines.the_geom))  As clipped
	WHERE ST_Dimension(clipped.clipped_geom) = 1 ;
