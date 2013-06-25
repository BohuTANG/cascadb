file( GLOB_RECURSE SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        "*.cpp"
		)
		
add_definitions("-Wno-sign-compare -Wno-unused-function -fPIC")
add_library(snappyShared SHARED ${SOURCE})
add_library(snappyStatic STATIC ${SOURCE})
set_target_properties(snappyShared snappyStatic PROPERTIES OUTPUT_NAME snappy)
