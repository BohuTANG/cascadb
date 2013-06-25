file( GLOB_RECURSE SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        "*.c"
		)
		

add_definitions("-fPIC")
add_library(quicklzShared SHARED ${SOURCE})
add_library(quicklzStatic STATIC ${SOURCE})
set_target_properties(quicklzShared quicklzStatic PROPERTIES OUTPUT_NAME quicklz)
