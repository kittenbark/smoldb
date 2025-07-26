package smoldb_test

import (
	"github.com/kittenbark/smoldb"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
)

func TestSmolStringString(t *testing.T) {
	smol, err := smoldb.New[string, string]("test_1.yaml")
	defer func() {
		if !t.Failed() {
			if err := os.Remove("test_1.yaml"); err != nil {
				panic(err)
			}
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := smol.Set(strconv.FormatInt(int64(i), 10), strconv.Itoa(i*i)); err != nil {
			t.Fatal(err)
		}
		val, err := smol.Get(strconv.FormatInt(int64(i), 10))
		if err != nil || val != strconv.Itoa(i*i) {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		if err := smol.Del(strconv.FormatInt(int64(i), 10)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSmolIntInt(t *testing.T) {
	smol, err := smoldb.New[int, int]("test_2.yaml")
	defer func() {
		if err := os.Remove("test_2.yaml"); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := smol.Set(i, i*i); err != nil {
			t.Fatal(err)
		}
		val, err := smol.Get(i)
		if err != nil || val != i*i {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		if err := smol.Del(i); err != nil {
			t.Fatal(err)
		}
	}
	keys := smol.Keys()
	slices.Sort(keys)

	if len(keys) != smol.Size() || smol.Size() != 50 {
		t.Fatal("unexpected keys size")
	}
	for i := 50; i < 100; i++ {
		val, err := smol.Get(i)
		if err != nil || val != i*i {
			t.Fatal(err)
		}
	}
}

func BenchmarkSmolIntInt(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, 4))

	if err := os.RemoveAll("test_3.yaml"); err != nil {
		panic(err)
	}
	smol, err := smoldb.New[int, int]("test_3.yaml")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if !b.Failed() {
			if err := os.Remove("test_3.yaml"); err != nil {
				panic(err)
			}
		}
	}()

	for b.Loop() {
		i := rng.IntN(100)
		switch rng.IntN(3) {
		case 0:
			if err := smol.Set(i, i*i); err != nil {
				b.Fatal(err)
			}
		case 1:
			if _, _, err := smol.TryGet(i); err != nil {
				b.Fatal(err)
			}
		case 2:
			if err := smol.Del(i); err != nil {
				b.Fatal(err)
			}
		}
	}
	println("reads ", smoldb.StatsRead.Load())
	println("writes ", smoldb.StatsWrite.Load())
}

func BenchmarkSmolCompareWithNanodb(b *testing.B) {
	smol, err := smoldb.New[string, string]("test_4.yaml")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove("test_4.yaml")

	i := 0
	keysN := len(testKeys)

	for b.Loop() {
		i++
		switch rand.N(3) {
		case 0:
			if err := smol.Set(testKeys[rand.N(keysN)], testKeys[rand.N(keysN)]); err != nil {
				b.Fatal(err)
			}
		case 1:
			if _, _, err := smol.TryGet(testKeys[rand.N(keysN)]); err != nil {
				b.Fatal(err)
			}
		case 2:
			if err := smol.Del(testKeys[rand.N(keysN)]); err != nil {
				b.Fatal(err)
			}
		}
	}
	println("reads ", smoldb.StatsRead.Load())
	println("writes ", smoldb.StatsWrite.Load())
}

var testKeys = []string{
	"green", "cyan", "blue", "red", "yellow", "purple", "orange", "pink",
	"brown", "black", "white", "gray", "magenta", "violet", "indigo",
	"teal", "turquoise", "navy", "maroon", "olive", "lime", "coral",
	"salmon", "gold", "silver", "beige", "khaki", "tan", "chocolate",
	"crimson", "lavender", "plum", "ivory", "azure", "mint", "forest",
	"burgundy", "coffee", "rust", "amber", "charcoal", "platinum", "ruby",
	"emerald", "sapphire", "amethyst", "topaz", "jade", "pearl", "onyx",
	"obsidian", "garnet", "aquamarine", "opal", "quartz", "bronze", "copper",
	"brass", "steel", "titanium", "cobalt", "nickel", "zinc", "aluminum",
	"mercury", "lead", "tin", "iron", "chrome", "tungsten", "mahogany",
	"oak", "pine", "cedar", "birch", "maple", "walnut", "cherry", "ash",
	"willow", "bamboo", "rosewood", "elm", "hickory", "apple", "banana",
	"orange", "grape", "strawberry", "blueberry", "raspberry", "blackberry",
	"lemon", "lime", "mango", "kiwi", "pineapple", "peach", "pear", "plum",
	"apricot", "cherry", "watermelon", "cantaloupe", "honeydew", "fig",
	"date", "coconut", "avocado", "tomato", "potato", "carrot", "celery",
	"spinach", "lettuce", "broccoli", "cauliflower", "corn", "pea", "bean",
	"lentil", "rice", "wheat", "barley", "oat", "rye", "quinoa", "bread",
	"pasta", "noodle", "pizza", "burger", "sandwich", "taco", "burrito",
	"sushi", "curry", "soup", "salad", "stew", "roast", "steak", "chicken",
	"turkey", "duck", "goose", "pork", "beef", "lamb", "fish", "shrimp",
	"crab", "lobster", "oyster", "mussel", "clam", "scallop", "squid",
	"octopus", "jellyfish", "starfish", "coral", "seaweed", "shell", "sand",
	"beach", "ocean", "sea", "lake", "river", "stream", "pond", "pool",
	"waterfall", "mountain", "hill", "valley", "canyon", "cliff", "cave",
	"desert", "forest", "jungle", "rainforest", "tundra", "prairie", "meadow",
	"field", "garden", "park", "yard", "lawn", "sidewalk", "street", "road",
	"highway", "path", "trail", "bridge", "tunnel", "building", "house",
	"apartment", "condo", "mansion", "cabin", "cottage", "castle", "palace",
	"temple", "church", "cathedral", "mosque", "synagogue", "skyscraper",
	"tower", "monument", "museum", "library", "school", "college", "university",
	"hospital", "clinic", "pharmacy", "store", "shop", "mall", "market",
	"restaurant", "cafe", "bar", "pub", "club", "theater", "cinema", "stadium",
	"arena", "park", "zoo", "aquarium", "circus", "carnival", "fair", "festival",
	"parade", "concert", "play", "opera", "ballet", "dance", "sing", "music",
	"art", "painting", "drawing", "sketch", "sculpture", "statue", "photograph",
	"film", "movie", "show", "book", "novel", "story", "poem", "letter", "essay",
	"report", "paper", "journal", "magazine", "newspaper", "comic", "manual",
	"guide", "map", "atlas", "globe", "compass", "calculator", "computer", "laptop",
	"tablet", "phone", "camera", "radio", "television", "clock", "watch", "calendar",
	"wallet", "purse", "bag", "backpack", "luggage", "umbrella", "hat", "cap",
	"crown", "helmet", "mask", "glasses", "sunglasses", "glove", "mitten", "scarf",
	"tie", "shirt", "blouse", "sweater", "jacket", "coat", "vest", "dress", "skirt",
	"pants", "jeans", "shorts", "sock", "shoe", "boot", "sandal", "slipper", "ring",
	"necklace", "bracelet", "earring", "pendant", "brooch", "comb", "brush", "soap",
	"shampoo", "towel", "toothbrush", "toothpaste", "floss", "razor", "mirror", "perfume",
	"cologne", "makeup", "lipstick", "nail", "hair", "beard", "mustache", "eye", "ear",
	"nose", "mouth", "lip", "tooth", "tongue", "throat", "neck", "shoulder", "arm", "elbow",
	"wrist", "hand", "finger", "thumb", "nail", "heart", "lung", "liver", "kidney", "stomach",
	"intestine", "brain", "spine", "bone", "muscle", "skin", "blood",
}
