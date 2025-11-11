include_guard(GLOBAL)

add_library(libmpdec SHARED IMPORTED GLOBAL)
set_target_properties(libmpdec PROPERTIES
        IMPORTED_LOCATION "${DEPENDENCY_DIR}/mpdecimal/lib/libmpdec.so" # Adjusted path for the library
        INTERFACE_INCLUDE_DIRECTORIES "${DEPENDENCY_DIR}/mpdecimal/include")


add_library(libmpdec++ SHARED IMPORTED GLOBAL)
set_target_properties(libmpdec++ PROPERTIES
        IMPORTED_LOCATION "${DEPENDENCY_DIR}/mpdecimal/lib/libmpdec++.so" # Adjusted path for the library
        INTERFACE_INCLUDE_DIRECTORIES "${DEPENDENCY_DIR}/mpdecimal/include")