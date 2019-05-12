package db

// difference 用来获取左边减去右边的差值
func difference(left []string, right []string) []string {
	var diff []string
	newRight := make(map[string]bool)
	for _, val := range right {
		newRight[val] = true
	}
	// 计算需要移除的
	for _, leftKey := range left {
		if _, ok := newRight[leftKey]; !ok {
			diff = append(diff, leftKey)
		}
	}

	return diff
}
