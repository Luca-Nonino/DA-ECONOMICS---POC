import pygame
import random

# Initialize Pygame
pygame.init()

# Set up the display
WIDTH, HEIGHT = 800, 600
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Vertical Platformer")

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
RED = (255, 0, 0)
BLUE = (0, 0, 255)
GREEN = (0, 255, 0)

# Player class
class Player(pygame.sprite.Sprite):
    def __init__(self):
        super().__init__()
        self.image = pygame.Surface((20, 40))
        self.image.fill(BLUE)
        self.rect = self.image.get_rect()
        self.rect.center = (WIDTH // 2, HEIGHT - 50)
        self.velocity_y = 0
        self.jumping = False
        self.ammo = 5

    def update(self):
        self.velocity_y += 0.5  # Gravity
        self.rect.y += self.velocity_y
        if self.rect.bottom > HEIGHT:
            self.rect.bottom = HEIGHT
            self.jumping = False

    def jump(self):
        if not self.jumping:
            self.velocity_y = -10
            self.jumping = True

    def move(self, dx):
        self.rect.x += dx
        if self.rect.left < 0:
            self.rect.left = 0
        if self.rect.right > WIDTH:
            self.rect.right = WIDTH

# Platform class
class Platform(pygame.sprite.Sprite):
    def __init__(self, x, y, width):
        super().__init__()
        self.image = pygame.Surface((width, 20))
        self.image.fill(GREEN)
        self.rect = self.image.get_rect()
        self.rect.x = x
        self.rect.y = y
        self.speed = random.randint(1, 3)
        self.direction = 1

    def update(self):
        self.rect.x += self.speed * self.direction
        if self.rect.left < 0 or self.rect.right > WIDTH:
            self.direction *= -1

# Fireball class
class Fireball(pygame.sprite.Sprite):
    def __init__(self, x, y):
        super().__init__()
        self.image = pygame.Surface((10, 10))
        self.image.fill(RED)
        self.rect = self.image.get_rect()
        self.rect.center = (x, y)
        self.velocity_y = -5

    def update(self):
        self.rect.y += self.velocity_y
        if self.rect.bottom < 0:
            self.kill()

# Enemy class
class Enemy(pygame.sprite.Sprite):
    def __init__(self, x, y):
        super().__init__()
        self.image = pygame.Surface((30, 30))
        self.image.fill(RED)
        self.rect = self.image.get_rect()
        self.rect.x = x
        self.rect.y = y
        self.direction = random.choice([-1, 1])
        self.speed = random.randint(1, 5)

    def update(self):
        self.rect.x += self.speed * self.direction
        self.rect.y += random.choice([-1, 1]) * self.speed
        if self.rect.left < 0 or self.rect.right > WIDTH:
            self.direction *= -1
        if self.rect.top < 0 or self.rect.bottom > HEIGHT:
            self.rect.y = random.randint(0, HEIGHT - self.rect.height)

# Ammo class
class Ammo(pygame.sprite.Sprite):
    def __init__(self, x, y):
        super().__init__()
        self.image = pygame.Surface((15, 15))
        self.image.fill(WHITE)
        self.rect = self.image.get_rect()
        self.rect.center = (x, y)

# Create sprite groups
all_sprites = pygame.sprite.Group()
platforms = pygame.sprite.Group()
fireballs = pygame.sprite.Group()
enemies = pygame.sprite.Group()
ammo_group = pygame.sprite.Group()

# Create player
player = Player()
all_sprites.add(player)

# Create platforms
for i in range(10):
    platform = Platform(random.randint(0, WIDTH - 100), HEIGHT - 100 * i, random.randint(50, 150))
    all_sprites.add(platform)
    platforms.add(platform)

# Create enemies
for i in range(5):
    enemy = Enemy(random.randint(0, WIDTH - 30), random.randint(0, HEIGHT - 200))
    all_sprites.add(enemy)
    enemies.add(enemy)

# Create ammo
for i in range(5):
    ammo = Ammo(random.randint(0, WIDTH), random.randint(0, HEIGHT))
    all_sprites.add(ammo)
    ammo_group.add(ammo)

# Game loop
running = True
clock = pygame.time.Clock()
last_enemy_spawn_time = pygame.time.get_ticks()

while running:
    # Handle events
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_SPACE:
                player.jump()
            elif event.key == pygame.K_f:
                if player.ammo > 0:
                    fireball = Fireball(player.rect.centerx, player.rect.top)
                    all_sprites.add(fireball)
                    fireballs.add(fireball)
                    player.ammo -= 1

    # Update
    all_sprites.update()

    # Player movement
    keys = pygame.key.get_pressed()
    if keys[pygame.K_LEFT]:
        player.move(-5)
    if keys[pygame.K_RIGHT]:
        player.move(5)

    # Collision detection
    if pygame.sprite.spritecollide(player, platforms, False):
        player.rect.bottom = pygame.sprite.spritecollide(player, platforms, False)[0].rect.top
        player.velocity_y = 0
        player.jumping = False

    # Fireball-enemy collision
    for fireball in fireballs:
        hit_enemies = pygame.sprite.spritecollide(fireball, enemies, True)
        if hit_enemies:
            fireball.kill()

    # Player-enemy collision (lethal)
    if pygame.sprite.spritecollide(player, enemies, False):
        running = False  # End the game if player hits an enemy

    # Player-ammo collision
    ammo_collected = pygame.sprite.spritecollide(player, ammo_group, True)
    for ammo in ammo_collected:
        player.ammo += 1

    # Spawn new enemy every 2 seconds
    current_time = pygame.time.get_ticks()
    if current_time - last_enemy_spawn_time > 2000:
        enemy = Enemy(random.randint(0, WIDTH - 30), random.randint(0, HEIGHT - 200))
        all_sprites.add(enemy)
        enemies.add(enemy)
        last_enemy_spawn_time = current_time

    # Draw
    screen.fill(BLACK)
    all_sprites.draw(screen)

    # Draw ammo count
    font = pygame.font.Font(None, 36)
    ammo_text = font.render(f"Ammo: {player.ammo}", True, WHITE)
    screen.blit(ammo_text, (10, 10))

    # Update display
    pygame.display.flip()

    # Cap the frame rate
    clock.tick(60)

# Quit Pygame
pygame.quit()
