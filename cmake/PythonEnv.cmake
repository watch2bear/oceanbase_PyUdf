# IMBridge_CMake
ob_define(PYTHON_DIR "/usr/local/python/3.11-withssl")

set(PYTHON_LIB_DIR "${PYTHON_DIR}/lib")

message(STATUS "Set Python dir ${PYTHON_DIR}")

add_library(python_lib INTERFACE)
target_include_directories(
  python_lib INTERFACE
  "${PYTHON_DIR}/include/python3.11"
  "${PYTHON_LIB_DIR}"
  "${PYTHON_LIB_DIR}/python3.11/site-packages/numpy/core/include/"
  "${PYTHON_LIB_DIR}/python3.11/site-packages/numpy/_core/include/")
target_link_libraries(python_lib INTERFACE
    -L${PYTHON_DIR}/lib -lpython3.11 -lpthread -ldl  -lutil -lm  -Xlinker -export-dynamic)