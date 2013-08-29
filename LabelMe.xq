module namespace labelme = "labelme";

declare document management function labelme:addAnnotationFile($doc as xs:string, $name as xs:string)
{ pf:add-doc($doc, $name, 10) };

declare document management function labelme:addAnnotationFile($doc as xs:string, $name as xs:string, $collection as xs:string)
{ pf:add-doc($doc, $name, $collection, 10) };

declare document management function labelme:addAnnotationFiles($uris as xs:string*, $docs as xs:string*, $col as xs:string) 
{ 
	for $i in 1 to count($uris)  
		let $uri := exactly-one($uris[$i])
		let $doc := exactly-one($docs[$i])
    	return pf:add-doc($uri, $doc, $col, 10)
};

declare document management function labelme:addAnnotationFiles2($uris as xs:string*, $docs as xs:string*, $cols as xs:string*) 
{ 
	for $i in 1 to count($uris)  
		let $uri := exactly-one($uris[$i])
		let $doc := exactly-one($docs[$i])
		let $col := exactly-one($cols[$i])
    	return pf:add-doc($uri, $doc, $col, 10)
};

declare document management function labelme:deleteDocument($doc as xs:string) 
{ pf:del-doc($doc) };

declare function labelme:getAnnotations() as xs:string*
{
	for $i in (pf:documents()/text())
		for $j in (doc($i)/annotation/object/name/text())
			return <annotationValue> { $j } </annotationValue>
};

(: This function return which documents contain a object annotated with the annotation
given in parameter. There is no explode of the annotation "string-flavour" and no keyword search,
this function consider annotation like free-text annotation :)
declare function labelme:searchByAnnotation($annotation as xs:string) as xs:string*
{ 
	for $i in (pf:documents()/text()) 
	where doc($i)/annotation/object/name=$annotation
		return <LabelMeDocument><folder> { doc($i)/annotation/folder/text() } </folder><filename> { doc($i)/annotation/filename/text() } </filename></LabelMeDocument>
};

declare updating function labelme:addVisualInstance($filename as xs:string, $new_instance as item()*)
{
	do insert $new_instance as last into doc($filename)//annotation
};